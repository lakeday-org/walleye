"""Position-preserving compatibility for two verified tree-sitter-bash defects.

Validate the untouched input with Bash's parse-only mode before constructing a
tree through equivalent grammar shapes. Source files are never rewritten, shell
commands are never executed, and token text/positions always refer to the input.
"""

import re
import shutil
import subprocess

PROFILE = "tree-sitter-bash-compat-v1"


class OriginalNode:
    def __init__(self, node, source: bytes, here_strings: set[int]):
        self.node = node
        self.source = source
        self.here_strings = here_strings

    def __getattr__(self, key):
        return getattr(self.node, key)

    def _wrap(self, node):
        return OriginalNode(node, self.source, self.here_strings) if node is not None else None

    @property
    def start_byte(self):
        start = self.node.start_byte
        return start - 2 if self.node.type == "<" and start - 2 in self.here_strings else start

    @property
    def text(self):
        return self.source[self.start_byte : self.end_byte]

    @property
    def children(self):
        return [self._wrap(child) for child in self.node.children]

    @property
    def named_children(self):
        return [self._wrap(child) for child in self.node.named_children]

    @property
    def parent(self):
        return self._wrap(self.node.parent)

    def child_by_field_name(self, name):
        return self._wrap(self.node.child_by_field_name(name))


def parse_compatible(source: bytes, parser):
    """Return a complete original-position tree, or None to retain diagnostics."""
    proxy = source.replace(b"<<<", b"  <")
    proxy = re.sub(rb"([#%])([\[\]])(?=\})", rb"\1x", proxy)
    if proxy == source:
        return None
    bash = shutil.which("bash", path="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin")
    if bash is None:
        return None
    try:
        validation = subprocess.run(
            [bash, "--noprofile", "--norc", "-np"],
            input=source,
            capture_output=True,
            timeout=5,
            env={"PATH": "/usr/bin:/bin", "LC_ALL": "C"},
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if validation.returncode:
        return None
    root = parser.parse(proxy).root_node
    if root.has_error:
        return None
    return OriginalNode(root, source, {m.start() for m in re.finditer(b"<<<", source)})
