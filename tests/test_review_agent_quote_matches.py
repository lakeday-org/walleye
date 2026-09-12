from walleye.review_agent import quote_matches


def test_raw_numeric_colon_source_line_falls_back_to_literal_match():
    lines = ["header", '    1: "one",']

    assert quote_matches(lines[1], lines, 2, 2)


def test_numbered_quote_accepts_a_label_at_the_cited_range_boundary():
    lines = ["header", "    value = 1", "footer"]

    assert quote_matches("2: value = 1", lines, 2, 2)


def test_normalized_raw_quote_matches_without_numbered_labels():
    lines = ["header", "    value = x + 1"]

    assert quote_matches("value =   x + 1", lines, 2, 2)


def test_fabricated_numbered_quote_is_not_accepted_by_literal_fallback():
    lines = ["header", "    value = 1"]

    assert not quote_matches("2: fabricated", lines, 2, 2)
