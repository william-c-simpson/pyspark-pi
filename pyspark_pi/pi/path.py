import re

_PATH_RE = re.compile(
    r'^(?:\\\\(?P<server>[^\\]+)(?=\\))?(?(server)\\|\\?)(?P<rest>[^\\]+(?:\\[^\\]+)*)$'
)

class Path:
    def __init__(self, path: str) -> None:
        m = _PATH_RE.match(path)
        if not m:
            raise ValueError(f"Invalid path format: {path}")
        if m.group("server") == ".":
            raise ValueError(f"Invalid server name: {m.group('server')}. You must use fully-qualified server names.")
        self.server = m.group("server")
        self.parts = m.group("rest").split("\\")

    def __str__(self) -> str:
        parts_str = "\\".join(self.parts)
        return f"\\\\{self.server}\\{parts_str}" if self.server else parts_str