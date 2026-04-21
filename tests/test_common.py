from common import parse_track


def test_smoke():
    result = parse_track("CARLOS DI SARLI * SENTIMIENTO CRIOLLO * 1941 *")
    assert result['orchestra'] == "CARLOS DI SARLI"
    assert result['track_title'] == "SENTIMIENTO CRIOLLO"
    assert result['year'] == 1941


def test_fascia_card_has_no_track_title():
    """Una fascia ID card non deve avere track_title."""
    result = parse_track("* * 1915*1985 *  *  *")
    assert result['track_title'] is None


def test_fascia_card_three_asterisks():
    """Variante con tre asterischi iniziali."""
    result = parse_track("* * * RANA FELICE *  *  *")
    assert result['track_title'] is None
