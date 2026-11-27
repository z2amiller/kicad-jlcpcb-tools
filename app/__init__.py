"""What the plugin does, apart from how a window shows it.

``PluginSession`` is what a view holds. Each operation reads the board
through the backend, changes it with one batch, and returns what changed,
for the view to show; none opens a dialog. A step that needs the user
returns what it found and takes the user's choice back as an argument.
Problems an operation meets reach the view through its ``Notifier``.

Nothing in this package imports wx, so another kind of view can use it.
"""
