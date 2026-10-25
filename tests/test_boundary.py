import ast
import sys
from pathlib import Path


def test_runtime_import_boundary():
    roots = sys.stdlib_module_names | {"nautilus_trader", "futu"}
    for path in Path("src/nautilus_futu").rglob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports = [name.name for name in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                imports = [node.module]
            else:
                continue
            for name in imports:
                assert name.split(".")[0] in roots, (path, name)
                if name.split(".")[0] == "futu":
                    assert path.name == "transport.py"
    for path in Path("examples").glob("*.py"):
        assert "import futu" not in path.read_text()
