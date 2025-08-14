import pytest
from pyspark_pi import pi

@pytest.mark.parametrize("path, server, parts", [
    (r"\\Srv\Db\Elem|Attr", "Srv", ["Db", "Elem|Attr"]),
    (r"\\Server123\X\Y\Z", "Server123", ["X", "Y", "Z"]),
    (r"\\{5c64}\Db", "{5c64}", ["Db"]),
    (r"\Db\Elem", None, ["Db", "Elem"]),
    (r"Db\Elem", None, ["Db", "Elem"]),
    (r"Db\Elements[@Category=Tutorial]|Volume", None, ["Db", "Elements[@Category=Tutorial]|Volume"]),
])
def test_path_parse_valid(path, server, parts):
    p = pi.Path(path)
    assert p.server == server
    assert p.parts == parts

@pytest.mark.parametrize("path, expected", [
    (r"\\Srv\Db\Elem|Attr", r"\\Srv\Db\Elem|Attr"),
    (r"\Db\Elem", r"Db\Elem"),
    (r"Db\Elem", r"Db\Elem"),
    (r"\\Server123\X\Y\Z", r"\\Server123\X\Y\Z"),
])
def test_path_str(path, expected):
    assert str(pi.Path(path)) == expected

@pytest.mark.parametrize("invalid_input", [
    r"",
    r"\\",
    r"\\Srv",
    r"\\Srv\\",
    r"Db\\Elem",
    r"\\.\Db\Elem",
    r"\\"
])
def test_path_invalid(invalid_input):
    with pytest.raises(ValueError):
        pi.Path(invalid_input)
