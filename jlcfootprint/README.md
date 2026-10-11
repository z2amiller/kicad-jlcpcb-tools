# jlcfootprint

JLC places each part by the EasyEDA footprint of the LCSC part number you picked, not by your KiCad footprint. This package works out a part's CPL rotation from that footprint: it aligns the KiCad footprint's pads onto JLC's pads, and the turn that aligns them is the rotation the CPL needs. The same alignment says whether the picked part fits the pads at all.

Everything here is plain Python 3.9, so it runs inside KiCad's own interpreter. Nothing imports wx. `kicad_adapter.board_parts` imports pcbnew only when it is called without it, and `easyeda_client` imports `requests`, which the plugin already uses, only to make a request; everything else is the standard library. Their tests (`tests/test_jlcfootprint_*.py`) need no network and no KiCad: EasyEDA's answers and KiCad's library footprints are recorded under `tests/fixtures/jlcfootprint/`.

| Module | What it holds |
| --- | --- |
| `geometry.py` | The `Pad` record, unit and frame conversions, the pad fingerprint, the angle helpers the solver and quality share, and the two conversions to CPL angles |
| `solver.py` | The 2D rigid alignment of two pad sets |
| `quality.py` | The angular RMS, a measure of how well a rotation explains the pads |
| `naming.py` | EasyEDA's footprint naming rule: the pin-1 position a package name encodes |
| `easyeda_parse.py` | EasyEDA's answers read into records: the batch lookup, footprint and symbol documents, the classic per-part response |
| `records.py` | The small records judging a part needs without the parsers: symbol pins, the pin-1 labels, the drawings' marks |
| `drawing.py` | The `+` marks and the body outline read from EasyEDA's symbol and footprint drawings |
| `boardfile.py` | Footprints, pads and courtyards read from KiCad board and footprint files, without pcbnew |
| `polarity.py` | What kind of two-pad part a part is, and which of its pads is the cathode or the positive terminal |
| `fit.py` | Pad pairing, the placement, the fit grades, JLC's package origin and the courtyard overhang |
| `resolver.py` | One part's verdict: its rotation, fit and polarity |
| `resolving.py` | The resolver called on a parsed EasyEDA record |
| `kicad_adapter.py` | A live pcbnew board read into `BoardPart`s: the resolver's inputs, the LCSC number and the verdict key |
| `easyeda_client.py` | The EasyEDA Pro client: the batch lookup, the footprint and symbol documents, the backoff |
| `worker.py` | The background fetch thread: two paced queues and a breaker |
| `cache.py` | The global cache of EasyEDA's answers |
| `verdicts.py` | The per-project verdict store in `project.db` |
| `model.py` | `Decision`, what the CPL emits for one reference |
| `presentation.py` | How the plugin words the check for the user: `describe_seconds` |
| `controller.py` | One open board's check: scanning, the fetch results, the decisions |

## Frames and units

- A `Pad` is in millimetres, in the footprint's own frame, with KiCad's Y axis pointing down. Its `rotation` is the pad's own angle within the footprint.
- EasyEDA's classic footprint data is in canvas units of 10 mil (0.254 mm), also with Y down. `easyeda_pads_to_mm` converts it once and does not flip Y (`FLIP_EASYEDA_Y = False`). EasyEDA Pro's footprint text is in mils with Y up and is flipped when it is read.
- A bottom-side footprint's pads are mirrored across the X axis back into the footprint's own frame (`mirror_y`, `mirror_box`), so both board sides compare the same way.
- `rotate` turns +X towards +Y. That is counter-clockwise in the usual Y-up picture, so in KiCad's Y-down frame a positive angle reads clockwise on screen.
- `pad_hash` fingerprints a footprint's pads: names, positions and sizes after each pad's own rotation, rounded to 0.001 mm, in any order. Pin functions are left out, so rewiring a schematic does not change it. It is meant as a persistent key for results worked out from those pads.

## The CPL rotation's sign

The solver's angle is the turn that carries KiCad's pads onto JLC's drawing, which reads clockwise on screen. The CPL counts the other way round: the counter-clockwise turn, as KiCad counts rotation, that carries JLC's drawing back onto KiCad's pads. Those are the same number, so `ccw_correction` only snaps the angle to the nearest 90 degrees and reports it as 0, 90, 180 or 270.

JLC's placement preview is the evidence. A SOIC-8 whose package name carries a -BL token (pin 1 at the bottom-left of JLC's drawing) needs 270 on a footprint at 0 degrees, where the negated angle would be 90. A SOT-23 needs 180, which reads the same either way.

## The alignment solver

`solver.solve_transform` takes two pad sets as `{name: (x, y)}`, pairs pads by name, and returns the rotation and translation that best carry the first set onto the second.

- It is the closed-form 2D Kabsch fit: the best proper rotation comes straight from the cross-covariance of the two centred sets through `atan2`. There is no SVD and no numpy.
- `rotation_deg_raw` is the exact angle and `rotation_deg` the nearest multiple of 90. `residual` is the RMS distance between paired pads at the raw angle.
- Pads are paired by equal names. A `Pad`'s number is a string, so lettered pads (A1, B12) pair too. A pad on only one side is ignored. Unnamed pads (NPTH holes, paste-only copper) cannot be paired; `geometry.named_pads` leaves them out.
- **The mirror flag.** The solver also fits the mirror image of the first set. When that fit's residual is under a tenth of the best rotation's, the result is flagged `is_mirrored`, but the reflection is never applied: a mirrored pin order is a finding about the footprint, never a rotation. A two-pad part fits both ways exactly, and a small floor keeps that from counting as a mirror.
- Fewer than two paired pads, pads that all sit on one point, or a non-finite coordinate give an underdetermined result instead of an exception.

## The quality measure

`quality.angular_rms` asks how well a rotation explains the paired pads. For each pad it compares the pad's bearing from its own side's centroid with its partner's bearing, after taking the rotation away, and returns the RMS of those errors in degrees. A wrong rotation shows up as a large error, and unlike the solver's residual it does not grow with the size of the part.

A pad close to the centroid (within a tenth of the pads' mean distance from it, such as an exposed pad or a DPAK tab) has a bearing that a hundredth of a millimetre can swing by tens of degrees, so it is left out.

## EasyEDA's naming rule

EasyEDA's Footprint Naming Rule puts pin 1's position into the package name: `{family}[_{dimensions}]-{token}`, as in `SOIC-8_L4.9-W3.9-P1.27-LS6.0-BL`. `naming.parse_package_name` reads it and returns a rotation, the rotation's source (`naming_rule`, `family_default` or `none`) and a confidence.

- **Tokens.** TL, TR, BL and BR put pin 1 at a corner; L, R, T and B at an edge; FD, RD and BI give the direction of a two-terminal polarized part.
- **The family** is the name up to its first underscore (or the leading letters of a code such as `LED0603-RD`), cut at its first dimensional segment, so `SMF-L2.9-W1.9-LS3.9-FD` and `SMF_L2.9-W1.9-LS3.9-FD` are both SMF.
- **Only real tokens count.**
  - Dimensional segments (L4.9, W3.9, LS6.0, P1.27, BD6.3, EP) are never orientations.
  - Connectors, headers, relays and other families whose suffix is a free manufacturer part number yield no token at all. Of the MICRO families only the connectors and card sockets (MICRO-USB, MICRO-B, MICRO-SD, MICRO-SIM) are listed, so MICRO-8 and MICRO-MELF keep their tokens.
  - Only the segment between the first and second underscore carries tokens (`VSON-10_L3.0-W3.0-TL-EP_TPS61230DRCR`), except in names that put them before the underscore (`LED0603-RD_GREEN`).
  - A name with no underscore is read only when it is shaped like the rule's names: a family code with digits (`LED0603-RD`) or dimensional segments (`SOP-5-L4.4-W3.6-P1.27-LS7.0-TL`). Anything else is a bare manufacturer part number, whose `-TR` (tape and reel) or `-L` is no orientation (`MICROFC-30035-SMT-TR`).
- **Which token wins.** A corner token (TL, TR, BL, BR) names where pin 1 is drawn, so it wins even over a polarity token: over EasyEDA's library, 81 of the 85 multi-pin drawings whose names carry both put pad 1 at the named corner. Otherwise a polarity token beats an edge token (L, R, T, B), since it names a physical direction, while an edge token on a two-terminal part is only as good as the library's choice of which terminal is pin 1.
- **Cathode-first families.** KiCad's LED and diode footprints number the cathode 1, so those families, the chip diodes named by size (`D0603-RD`) among them, read FD and RD the other way round, at medium confidence. A caller that knows a part is a diode can pass `cathode_is_pin1` for a family the table does not list. Polarized capacitors number the positive terminal 1 and keep the standard reading, and multi-LED arrays (LED-ARRAY and its SMD and TH variants) are never treated as cathode-first.
- **No token.** Standard passives (R, C, L, D, F) default to 0 at low confidence; anything else gets the source `none` and no confidence.
- **Sign.** The table counts the counter-clockwise turn that carries KiCad's footprint onto JLC's drawing, the opposite of the CPL, so `geometry.naming_rule_to_cpl` negates it: the -BL SOIC-8 is 90 in the table and 270 in the CPL.

The rule is a convention, and EasyEDA's library does not always follow it. Over EasyEDA's library, an FD or RD token agrees with the "+" marks drawn on the symbol and footprint on 201 of 203 electrolytic cans, but on only 39 of 45 molded tantalum chips. So the check treats a package name as one source among several, never as the answer on its own.

## Reading EasyEDA's data

`easyeda_parse.py` reads EasyEDA's answers. It makes no requests of its own.

- **The Pro host** (`pro.easyeda.com`) answers in two steps. The batch lookup (`searchByCodes`) takes up to 200 LCSC numbers and returns one record per part it knows: the symbol uuid, the footprint uuid and the package name (`parse_devices_response`). The per-uuid endpoint then returns one document, a footprint (`parse_puuid_response`) or a symbol (`parse_symbol_response`), in EasyEDA Pro's text form: one JSON array per line, in mils, with Y up. `assemble_record` joins a hit and its two documents into one `ComponentRecord`.
- **The classic host** answers per LCSC number, with the symbol and the footprint in one response (`parse_component_response`). Its shapes are `~`-separated strings in 10-mil canvas units, with Y down. The per-uuid endpoint still serves some footprints in this form, and `parse_puuid_response` reads both.
- **One pad form.** Pro pads are converted to classic canvas units as they are read: mils divided by ten, Y flipped, and a polygon pad's bounding box taken as its size. Everything downstream sees one raw pad form, which `geometry.easyeda_pads_to_mm` turns into millimetres. Leading zeros are stripped from numeric pad and pin numbers, so a Pro `01` and a classic `1` are one pad.
- **Pin numbers** are the numbers the symbol displays, not its SPICE field, because the displayed number is the one that matches the footprint's pads. Two parts can share a footprint and still have different symbols, so pin meaning is per part.
- **Status.** Each parser returns `ok`, `none` or `error`, and none of them raises. The body's `success` flag decides. `true` is an answer, and an answer with an empty result is `none`. `false` is EasyEDA's refusal: with no code or a not-found code it is `none` too. `none` means EasyEDA has nothing for the part; JLC's preview shows such a part as a checkerboard. A refusal with any other code, a body whose `success` is missing or not a boolean, and any answer that cannot be read are `error`, with the reason kept, so they are asked again later instead of being taken as EasyEDA's answer. A batch answer that is not a list of device records is an error for every code it was asked about, not a miss.

`records.py` holds what the drawings and the symbol's pins say (`DrawingMarks`, `SymbolPin`, the pin-1 label sets), so that code judging a part can read it without importing the parsers.

The recorded EasyEDA answers come from `tools/jlcfootprint/fetch_easyeda_fixture.py`. It paces its requests, backs off when the host refuses, and keeps an answer only when the parser reads it as `ok` or `none`.

### The drawings

No EasyEDA record says which terminal of a two-pad part is positive. The drawings do: the symbol draws a `+` beside its positive pin, and the footprint draws one beside its positive pad, on the silkscreen or the document layer. JLC's placement preview follows these marks. Where a tantalum's footprint numbers its terminals the other way round, the package name's FD/RD token is wrong and the marks are right.

`drawing.py` reads both drawings, in both formats, each in its own frame and units. Only two-pin symbols and two-pad footprints are read. A `+` is two bars, a horizontal one and a vertical one, on the same layer, and it has to stand alone:

- the bars are alike in length (neither is more than 2.5 times the other) and cross near both their midpoints, which rules out an outline corner and a `T`;
- they are between 5 % and 45 % of the pin span long on a symbol, and between 5 % and 70 % of the pad spacing on a footprint, so a capacitor's plates, a footprint outline and pin stubs are not bars of a `+`;
- on a footprint the lower bound never exceeds 0.75 mm, so the ordinary 30 to 40 mil mark beside a large part's pads is still read;
- no third bar comes near the crossing or the bars' ends, and no polyline or polygon edge comes near the crossing, which rules out a diode's silk arrow;
- on a footprint the crossing sits off the centre (an origin cross names no pad), and every `+` has to be on the same side;
- on a symbol the nearer pin has to be clearly nearer, and every `+` has to name the same pin.

When any of this fails, no terminal is named. The footprint's body outline (Pro layer 48, classic layer 99) is read as a box in millimetres, in the same frame as the pads.

## Reading KiCad files

`kicad_adapter.py` reads a live board's footprints through pcbnew, the ones its caller passes (the plugin's own selection is `footprint_helpers.get_valid_footprints`, the footprints with a reference), into one `BoardPart` each: its reference, LCSC number, footprint name, side and placed rotation, its soldered pads in the footprint's own frame, its courtyard box and its verdict key (see the verdict store below). The pads come from `GetFPRelativePosition` and `GetFPRelativeOrientation` where pcbnew has them (KiCad 8 and later), otherwise from the board position turned back by the footprint's angle; pcbnew stores a bottom-side footprint's pads mirrored, so they are mirrored back. The courtyard is the box of the courtyard lines' centrelines, taken back into the footprint frame. pcbnew is imported only when no module is passed in, so the tests drive the adapter with duck-typed footprints, and `lcsc_of` lets a caller read part numbers its own way.

The development tools and the tests read KiCad files from plain Python instead: `boardfile.py` parses the footprints of a `.kicad_pcb` or `.kicad_mod` file into their pads (in the footprint's own frame, with the placed rotation taken off), pin functions, LCSC field and courtyard box. The LCSC field is read as the plugin's `footprint_helpers.get_lcsc_value` reads it (`boardfile.lcsc_assignment`, which `kicad_adapter.lcsc_value` shares): only part-number field names count (LCSC, JLC or JLCPCB, with an optional Part, Part Number, PN, Code or the like), the number is upper-cased, and two fields naming different numbers are a conflict, read as no number. The pads are the soldered ones, the same set `kicad_adapter` keeps when it reads a board through pcbnew: every copper pad that is not an NPTH hole, numbered or not. A paste-only aperture is left out. The alignment pairs pads by name, so an unnumbered pad only counts in the pad-box centre. That centre is taken from the pads' positions and sizes, so it is the centre of pcbnew's pad bounding box except where the copper is not the pad's size at its position: a custom pad's extra primitives, copper offset from its drill, or a pad turned off the 90-degree grid. Over KiCad 10's library the pad sets agree on every footprint, and the centres on all but 385 of the 15,113 footprints with soldered pads: 358 with custom pads, 11 with offset copper, 2 with both and 14 with pads at other angles. The largest difference is 2.25 mm.

`tools/jlcfootprint/kicad_library.py` reads a named KiCad library footprint for the tests. It uses the snapshot recorded under `tests/fixtures/jlcfootprint/kicad/` when there is one, so the tests see the same pads on every machine and run without KiCad. `tools/jlcfootprint/snapshot_kicad_footprints.py` records the snapshots from an installed KiCad, and `test_jlcfootprint_snapshots.py` compares them with the installed library where there is one.

## Judging a part

`resolver.resolve` takes one footprint's KiCad pads (in the footprint's own frame, a bottom-side part mirrored back), its KiCad footprint name, EasyEDA's package name, pads in millimetres and symbol pins, and optionally the drawings' marks and the KiCad courtyard. It returns a `Verdict`: a status, a fit grade, the CPL rotation when one can be derived, how it was found (`method`), a confidence and notes that say why. `resolving.resolve_record` makes the same call from a parsed EasyEDA record.

A part EasyEDA has nothing for, a failed fetch, a pad with a non-numeric coordinate and a side with fewer than two named pads all give `unknown` with no rotation. Otherwise the part takes one of three paths.

### Parts with more than two pads

The pads pair by name and `solver.solve_transform` aligns them; the turn, snapped to 90 degrees, is the rotation (`method` `geometry`).

- A name that covers several pads on one side (KiCad numbers a SOT-223's tab as a second pad 2, a DPAK's tab and the stub of its cut lead both as 2, a switch its four legs 1, 1, 2, 2) pairs pad by pad: a pad first meets one of its own kind (within 1.5 times its area), so a pin meets a pin and a tab meets a tab, and among those the nearest after a first placement solved on the names that are unique on both sides (or, when fewer than two are, on each name's centroid). The order either footprint lists its pads in never matters. All KiCad pads that carry one number are one terminal and are graded together.
- When the numbers do not line up (fewer than two shared names, a mirrored order, or a pad that misses), the pads pair by the schematic's pin functions against the symbol's pin names instead: a name found once on each side pairs, at least two must pair, and one pad left on each side pairs by elimination. KiCad's `TO-252-3_TabPin2` numbers the tab 2 where JLC's DPAK drawing numbers it 3; its `D` function pairs it all the same. A pairing by function is medium confidence and its note lists every number that differs.
- Without that, a mirrored pin order is red (`mirror`), never solved. A part with as many pads as JLC's whose pads all land at some angle when their names are ignored is red (`numbering`), with the angle the package itself would take: the numbering differs from JLC's part, so the package's angle would put the wrong pins on the wrong pads.
- Only two shared names, or a rotation the package name's token disagrees with, lowers the confidence to medium, with a note.

### Two-pad parts by meaning

A part with two JLC pads that is polarized, or whose package name carries an FD/RD token, aligns by terminal meaning and never by pad number (`method` `polarity`).

- **The kind.** `polarity.part_kind` reads the evidence from the most to the least reliable: the KiCad footprint's name (`D_`, `LED_`; `CP_`, `C_Elec`, `Tantalum`) and its pin functions (`K`/`A`), EasyEDA's family (the cathode-first diode families, the polarized capacitor families), the battery, buzzer and microphone footprints and families, the symbol's pin labels, and last a `+` drawn in either drawing. A diode's reference terminal is its cathode; a capacitor's and any other polarized part's is its positive terminal.
- **The KiCad side.** A pin function naming the reference terminal, or naming the other terminal on the other pad, gives the reference pad (`K_1`, `+_1` and `anode` all count). Two pads claiming one terminal are contradictory and the part is unknown. With no usable function pad 1 is assumed (K on a diode, + on the other polarized parts), at medium confidence, and a note says so.
- **The EasyEDA side: the vote.** Up to four sources each name the pad that carries the reference terminal: the package name's FD/RD token (forward direction puts the marking band, the cathode or the negative end, on the right of JLC's drawing), the symbol's pin-1 label, the symbol's `+` and the footprint's `+`. The majority decides. A tie, such as one token against one mark, is red: "EasyEDA data inconsistent", and no guess. No source at all is unknown: "polarity unknown". A vote that loses is named in a note and lowers the confidence to medium, and so does a decision by the marks alone or an unchecked token on the molded-chip tantalums, the one family whose token is least reliable (over EasyEDA's library it agrees with the marks on 39 of 45 of them, against 201 of 203 electrolytic cans). A bidirectional (`BI`) part has no reference terminal and aligns by axis.
- Once both reference pads are known the two pads align like any pair, and that pairing must fit; a polarized part is never turned to fit by shape.
- **The pin-1 light.** The vote also says what JLC's pad 1 is. When it is not what KiCad's pad 1 is, the rotation is still right but JLC's pin-1 marker will sit on the other terminal, so the part is yellow with a note saying not to renumber the footprint.
- An FD/RD token on a part that is not polarized (a marked inductor) puts KiCad's pad 1 where the token puts pin 1, read the way a capacitor's token is read, which need not be JLC's pad 1, and the part is flagged non-polar.

### Two-pad parts without polarity

A non-polar part with two pads on each side aligns by its axis, and 180 degrees is irrelevant: the rotation is given modulo 180 (`method` `axis`).

### The second pairing

Two pads pair two ways, so both are graded: KiCad's two pads against JLC's, and against JLC's swapped. One pairing fits clearly better when it fits and the other does not, or when both fit and its worst pad's overlap is at least `fit.SHAPE_MARGIN` (0.1) higher. Mirror-symmetric pads give exactly 0, and so does every two-pad part on the gate boards; a coin-cell holder whose two pads differ in shape gives 0.263.

- A non-polar part whose pads fit one pairing clearly better takes that pairing and a full turn (`method` `shape`).
- A polarized part keeps its meaning's rotation, but when the swapped pairing fits clearly better it is yellow: one of the two footprints may draw its terminals at the other end.

### Fit and status

After the placement every JLC pad must land on KiCad copper of its number, overlapping it by at least half the smaller pad's area; a pad whose centre falls outside the inner 80 % of the KiCad pad lands tight. A JLC pad no name pairs is graded against the nearest KiCad pad; extra KiCad copper is never a misfit, and a custom-shaped KiCad pad is not judged.

| `fit` | Meaning |
| --- | --- |
| `fits` | Every JLC pad lands on KiCad copper |
| `fits_larger_pads` | It fits, and KiCad's pads are at least 1.5 times JLC's (hand-solder pads) |
| `fits_tight` | The pads overlap, but a JLC pad's centre sits at the edge of its KiCad pad |
| `count`, `pitch` | A JLC pad misses: `count` when the pad counts differ, `pitch` when they are equal |
| `numbering`, `mirror` | The package fits but the numbering differs, or runs the other way |
| `no_data` | Nothing to align |

The status is **green** when the part fits and its rotation is known, **yellow** when it fits with a caveat (tight pads, a central pad the KiCad footprint lacks, JLC's pin-1 marker on the other terminal, the shapes preferring the other pairing), **red** when it does not fit or EasyEDA's data contradicts itself, and **unknown** when there is nothing to judge. Only green and yellow carry a rotation.

- **The body caveat.** When the footprint drawing has a body outline and the KiCad footprint a courtyard, the courtyard is moved by the placement and compared with JLC's body. An overhang of more than 10 % of the courtyard's shorter side (never less than 0.15 mm nor more than 1.5 mm) is noted; the status and the rotation stand.
- **The package origin.** `Verdict.origin` is the point the placement carries onto the drawing's own origin: where JLC's package origin sits in the footprint frame, for green and yellow verdicts only.

## The gate

Two KiCad boards under `tests/fixtures/jlcfootprint/boards/` were uploaded to JLC, and for each part checked the rotation JLC's placement preview settled on was written down (D5's by construction, as its note says: it is D4 with the anode wired to pad 1, which the preview cannot show). The gate is that the resolver, run offline on the same boards and the recorded EasyEDA answers, reproduces every one of them.

- `corner_case/` has 47 footprints from KiCad's own library: SOT-23s at all four rotations and on both sides, SOIC-8, QFN, SOT-223, DPAK, DIP-8, USB-C, diodes, LEDs, electrolytic and tantalum capacitors, resistors, a 0603 part on 0402 pads, a wrong pick (SOT-23-5 on SOT-23-6 pads) and a SOT-23 whose JLC drawing numbers its pins clockwise (`-CW`). `tools/jlcfootprint/make_corner_case_board.py` builds it with pcbnew from `cases.csv` and `blank.kicad_pcb`.
- `pin_functions_and_marks/` has 16: DPAKs, most of whose JLC drawings number the tab differently, paired by pin function; tantalums whose polarity comes from their `+` marks and tokens, one on the bottom side; and an electrolytic on a footprint too small for its body, beside the same part on one that fits.

**The truth files.** `tests/fixtures/jlcfootprint/truth.csv` belongs to the corner-case board and `truth_pin_functions_and_marks.csv` to the second. Each row is one part:

- `observed_rotation` is JLC's angle for the part after it was aligned in the preview. Blank means the part must get no derived rotation (a wrong pick, or a refusal by design).
- `origin_dx_mm` and `origin_dy_mm`, where present, pin JLC's package origin in the footprint frame, within 5 µm; blank means no origin.
- `note` says why, and is not read.

The preview does not show an angle. It turns a part 90 degrees counter-clockwise per press of the space bar, so the corner-case board was uploaded with the raw placed angles and the presses were written down per part (`truth_presses.csv`); `tools/jlcfootprint/truth_from_presses.py` adds them to the uploaded CPL's angles.

**The validator.** `tools/jlcfootprint/validate_board.py` reads the board with `boardfile`, without pcbnew, and each part's recorded EasyEDA answer: the classic responses by default, or the Pro host's batch, footprint and symbol documents with `--pro-fixtures`. It resolves every part, works out the CPL rotation the plugin's `Fabrication` would write (a bottom-side angle mirrored as 180 minus the angle, then the correction added), and compares it with the truth. A part aligned by its axis, or a marked inductor, compares modulo 180; a part aligned by pad shape fits one way round only and compares modulo 360. Any disagreement, or a truth row for a part that is not on the board, exits 1. `--report` writes the per-part table; the committed `pre_upload_report.txt` beside each board is that table, so a change in any verdict, note or origin shows up as a diff.

```bash
B=tests/fixtures/jlcfootprint/boards; FX=tests/fixtures/jlcfootprint; T=tools/jlcfootprint
python3 $T/validate_board.py $B/corner_case/corner_case.kicad_pcb --truth $FX/truth.csv --report /tmp/r.txt && cmp /tmp/r.txt $B/corner_case/pre_upload_report.txt
python3 $T/validate_board.py $B/corner_case/corner_case.kicad_pcb --truth $FX/truth.csv --pro-fixtures $FX/easyeda_pro --report /tmp/r.txt && cmp /tmp/r.txt $B/corner_case/pre_upload_report.txt
python3 $T/validate_board.py $B/pin_functions_and_marks/pin_functions_and_marks.kicad_pcb --truth $FX/truth_pin_functions_and_marks.csv --pro-fixtures $FX/easyeda_pro --report /tmp/r.txt && cmp /tmp/r.txt $B/pin_functions_and_marks/pre_upload_report.txt
```

All 47 parts on the corner-case board agree, from the classic answers and from the Pro host's, with the same report; on the second board all 16 agree. `tests/test_jlcfootprint_gate.py` runs the same comparisons in the test suite, compares each report with the committed one byte for byte, and pins a few of the verdicts.

**The live reader.** A live board is read through pcbnew, not through `boardfile`, so `tools/jlcfootprint/check_live_adapter.py` holds the two readers to each other on the gate boards. Run with KiCad's own Python, it loads a board with pcbnew, reads it through `kicad_adapter`, and compares every footprint with `boardfile`'s reading: the pads (positions and sizes to 0.1 µm, angles modulo 180, pin functions), the courtyard box and the verdict key, and, for every part with a recorded answer, the status, fit, rotation and package origin the resolver returns from each. Any difference exits 1. On both boards every footprint agrees.

```bash
KPY=/Applications/KiCad/KiCad.app/Contents/Frameworks/Python.framework/Versions/Current/bin/python3
$KPY $T/check_live_adapter.py $B/corner_case/corner_case.kicad_pcb
$KPY $T/check_live_adapter.py $B/pin_functions_and_marks/pin_functions_and_marks.kicad_pcb --pro-fixtures $FX/easyeda_pro
```

## Fetching from EasyEDA

`easyeda_client.EasyEdaClient` makes the requests and `worker.FetchWorker` paces them on one daemon thread. Neither decides what to fetch: a caller queues LCSC numbers and uuids and handles each answer.

- **One host.** Every request goes to EasyEDA's Pro host, `pro.easyeda.com`. The classic host answers one part per request and starts refusing after a short burst, so nothing here calls it; its answers appear only among the recorded fixtures. The two hosts serve the same library: the corner-case board's gate report, for one, is the same from either host's recorded answers.
- **What is sent.** The batch lookup POSTs up to 200 LCSC numbers and the LCSC library's id on the Pro host (`LCSC_COMPANY_PATH`) to `/api/devices/searchByCodes`, and gets back each known part's symbol uuid, footprint uuid and package name. Each footprint and each symbol is then one GET of `/api/components/{uuid}`, with a uuid the lookup returned. The requests carry a browser-like `User-Agent` and an `Accept` header for JSON; there is no account, no cookie and nothing about the board beyond the part numbers. Each times out after 20 s.
- **Pacing.** Each queue has a token bucket. Lookups get a burst of two, then one every two seconds; documents a burst of ten, then one every 1 to 1.2 seconds, well under the 2.5 document requests per second the Pro host has answered over long runs through EasyEDA's library without a refusal. A lookup waits half a second after the newest code arrives, so a burst of assignments shares one call; a settled lookup goes before the documents, and footprints before symbols. Successive workers may share one pair of buckets (`Buckets`), so starting a new worker does not reset the pace. `estimate_seconds` gives a rough wait for what is queued, counting two documents for every code still to be looked up.
- **Backoff.** HTTP 403, 429, 500, 502, 503 and 504 wait for `Retry-After` when it is a number of seconds (at most ten minutes), otherwise 60, 120 and 240 s, and then fail. The client calls its `acquire` before every attempt, retries included, and its `wait` for each backoff; given the worker's `acquire` and `wait_for`, every attempt spends one of the worker's tokens and a stop request cuts any wait short.
- **Failures.** A network error, an unreadable body, a refusal of the batch lookup and a status still failing after the backoff are transient: the request may be asked again later. HTTP 404 is `none` (EasyEDA has nothing there), another 5xx is transient, and any other status is final. A document EasyEDA refuses is `none` or a final error, by the status rules under "Reading EasyEDA's data". A lookup's failure fails its whole chunk. None of the calls raises.
- **The breaker.** Three transient failures in a row, from either queue, trip the worker: it serves neither queue again, what is queued stays pending, and one message is logged and passed to `on_tripped`. A success in between resets the count.
- **Threads.** One lock guards the queues and the in-flight slot, so queueing and the queries are safe from any thread; a job and its result handler run on the worker's thread, outside the lock. A job leaves the in-flight slot before its handler runs, so a handler that queues the same key again is heard. A job that raises counts as a transient failure, and a handler that raises is logged; neither stops the thread.

## Storage

### The EasyEDA cache

`cache.Cache` keeps EasyEDA's answers in one sqlite file for every project (`cache.FILENAME`, `jlcfootprint-cache.db`), so a part is fetched once, not once per project.

- **`lcsc_map`**, one row per LCSC number: the status (`ok`, `none`, or `error` with its reason), the footprint uuid, the symbol uuid, the symbol's pins and its drawing, and when the row was written. Pin meaning is a per-part fact, so it is kept per part.
- **`package`**, one row per footprint uuid, shared by every part that uses the footprint: the package name, the pads in the parsers' raw form, the footprint drawing and its origin, where the footprint came from, and the naming rule's reading of the name. Pads are converted to millimetres only when a part is judged, so a change to the frame conventions never invalidates the cache. A classic drawing keeps its own origin (its head x/y, already subtracted from the pads) because its body outline and marks are read against it; Pro text's is (0, 0). Drawings are stored as zlib-compressed JSON.
- **Filled in steps.** What the batch lookup returns is stored first: a part's uuids (`store_lookup`; a part with no symbol gets an empty pin list) or a `none` row (`store_lookup_miss`). The footprint document then gives the package row (`store_footprint`, never one without pads) and the symbol document the pins (`store_symbol`). `needs(lcsc)` says what is still missing: `{"lookup"}`, some of `{"footprint", "symbol"}`, or nothing when the part can be judged from the cache. A footprint already stored for another part is not fetched again.
- **Refresh rules.** No row expires on a timer; `needs` decides. A `none` row is looked up again after 30 days (`NONE_RETRY_S`), an `error` row the next time the part is checked, and an `ok` row never, until `forget` drops the part's row (its shared footprint row stays) or `clear` drops everything.
- **The layout gate.** The file records `SCHEMA_VERSION` in its `meta` table. A file written under any other version, older or newer, is dropped and rebuilt when it is opened, not migrated: every row in it was fetched and can be fetched again, so a change to these tables only has to raise the version.
- Every call opens its own connection, with a five-second busy timeout, so the worker thread may write while the main thread reads.

### The verdict store

`verdicts.VerdictStore` keeps one table, `footprint_verdict`, in a project's own `jlcpcb/project.db`, beside the plugin's other project tables.

- **The key** is `(lcsc, footprint_hash)`, and the hash is `kicad_adapter.verdict_key`: the footprint's `pad_hash` when no pad has a pin function, otherwise a hash of the pad hash together with each pad's number and normalised pin function (`K_1` reads `K`, `+_1` reads `+`). Pin functions are part of the key because two footprints with the same pads but swapped K/A functions need opposite rotations, as the corner-case board's D4 and D5 do (one LCSC, one footprint). So editing a footprint's pads or swapping its pin functions orphans its verdict and its override together, by design, and the same part on the top and on the bottom shares one row, its pads mirrored back.
- **A row** holds the resolver's verdict (status, fit, rotation, method, confidence, the name's angle, the pin-1 light, the pad counts and the fit measurements, the courtyard overhang, JLC's package origin in the footprint frame, the notes, when it was resolved) and the user's override (`override_rotation`, `override_note`).
- **Writing.** `save` stores a verdict and rewrites every column but the override's; a green or yellow verdict writes its package origin and anything else writes none. `mark_pending` records that a part waits for its EasyEDA data: a new row is created pending, and an existing one turns pending with its rotation, method, fit and notes cleared, while its override, its origin columns and its other measurements stay until the next `save`. `set_override` sets or clears the user's rotation; `delete` drops a row, override included.
- **Reading.** `emitted_rotation` is the override, else the rotation of a green or yellow row, else None, which leaves the part at its raw angle; `source` says which (`override`, `derived`, `raw`). `origin` is JLC's package origin for a green or yellow row only, so a pending row reports none even though its columns are kept. `display_text` is the emitted rotation as text: `180°`, `90° !` for a yellow row, `270° set` for an override, or `raw`.
- A table written by a development build of the check lacks the courtyard-overhang and origin columns, and projects opened with those builds still hold one; it gains the columns when the store opens (`add_missing_columns`), its rows and overrides kept, and two KiCad processes doing so at once is not an error.
- Every call opens its own connection, with a five-second busy timeout, so a background thread may write while the main thread reads.

## The check session

`controller.FootprintCheck` runs the check for one open board: it owns the cache, the verdict store, the client and the worker, and reads the board through a `read_board` callable its caller passes.

- **Scanning.** `scan_board` reads the board on the caller's thread and takes each part with an LCSC number in turn. A part whose verdict is resolved and whose cache row is final is left alone; a part the cache can judge is resolved and its verdict saved; any other part's verdict is marked pending and what it still needs is queued: the batch lookup, or the footprint and symbol documents. A full scan starts a new generation. `enqueue_references` scans only the given references, after an assignment or a removed number. `ScanSummary` is the scan's log line: what was resolved, queued and pending, and roughly how long the queue takes (`presentation.describe_seconds`).
- **Results.** The worker's handlers store each answer in the cache. Once a part's row is complete, every board part with that LCSC is resolved and stored, and `post(lcsc, generation)` tells the caller; an event of an older generation belongs to a board state that is gone, and the caller drops it. A failed fetch becomes an error row and an unknown verdict, so the part keeps its raw angle and is looked up again the next time it is checked. A tripped breaker reaches the caller through `message`.
- **The lock.** One re-entrant lock makes each scan step and each result handler whole with respect to the other: a result landing mid-scan is seen still pending or complete, never half handled, and a scan never queues a part behind a request that has just been answered.
- **Decisions.** `decisions` re-reads the board and returns one `model.Decision` per reference, from the verdict row: the rotation the CPL applies (`emitted_rotation`: the override, else a green or yellow row's rotation, else none), where it comes from (`source`), the status, fit, notes, the courtyard overhang, JLC's package origin and whether the part is still pending. A part without an LCSC is `no-lcsc` and a part with no row `no-verdict`. `display_text` is the Rotation column's text: `180°`, `90° !` for a yellow part, `270° set` for an override, or `raw`.

## What comes next

The package arrives in parts. Still to come:

- **The CPL path**: CPL rotations from the check, behind a setting that is off by default.
- **The UI**: a JLC column with a hover, a detail dialog with a pad overlay, overrides and actions.
- **Variant boards**: the check on boards with KiCad design variants.
