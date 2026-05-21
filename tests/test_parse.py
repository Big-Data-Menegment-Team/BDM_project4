"""Tests for the pure parsing helpers in rico.tasks.parse.

`parse_hierarchy` and `text_representation` are translated from Section 2 of the
lab notebook; these tests pin that behaviour so the translation stays honest.
"""

import json

from rico.tasks.parse import parse_hierarchy, text_representation


def test_parse_unwraps_activity_root_wrapper():
    raw = json.dumps(
        {
            "activity": {
                "root": {
                    "class": "android.widget.Button",
                    "text": "Sign in",
                    "bounds": [0, 0, 100, 40],
                }
            }
        }
    )
    assert parse_hierarchy(raw) == [("Button", "Sign in", (0, 0, 100, 40))]


def test_parse_works_without_activity_wrapper():
    raw = json.dumps(
        {"class": "android.widget.Button", "text": "Sign in", "bounds": [0, 0, 100, 40]}
    )
    assert parse_hierarchy(raw) == [("Button", "Sign in", (0, 0, 100, 40))]


def test_parse_element_type_is_class_last_segment():
    raw = json.dumps({"class": "com.example.ui.widgets.FancyTextView", "text": "Hi"})
    (element_type, _text, _bounds), = parse_hierarchy(raw)
    assert element_type == "FancyTextView"


def test_parse_skips_nodes_with_neither_text_nor_class():
    raw = json.dumps(
        {
            "class": "Root",
            "children": [
                {},  # no text, no class -> dropped
                {"text": "kept", "bounds": [1, 2, 3, 4]},
            ],
        }
    )
    elements = parse_hierarchy(raw)
    assert ("", "kept", (1, 2, 3, 4)) in elements
    assert len(elements) == 2  # Root (has class) + the text node; empty {} dropped


def test_parse_defaults_bounds_when_malformed():
    raw = json.dumps({"class": "Box", "text": "x", "bounds": [1, 2, 3]})  # length 3
    (_type, _text, bounds), = parse_hierarchy(raw)
    assert bounds == (0, 0, 0, 0)


def test_parse_recurses_into_nested_children():
    raw = json.dumps(
        {
            "class": "Layout",
            "children": [
                {"class": "Group", "children": [{"text": "deep", "bounds": [5, 5, 6, 6]}]}
            ],
        }
    )
    texts = [text for _type, text, _bounds in parse_hierarchy(raw)]
    assert "deep" in texts


def test_text_representation_orders_by_reading_position():
    # bounds = (x_left, y_top, x_right, y_bottom); reading order sorts by (y_top, x_left).
    elements = [
        ("T", "bottom", (0, 100, 10, 110)),
        ("T", "top-left", (0, 0, 10, 10)),
        ("T", "top-right", (50, 0, 60, 10)),
    ]
    assert text_representation(elements) == "top-left top-right bottom"


def test_text_representation_skips_empty_text():
    elements = [("T", "", (0, 0, 1, 1)), ("T", "real", (0, 10, 1, 11))]
    assert text_representation(elements) == "real"
