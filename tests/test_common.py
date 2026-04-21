from common import parse_track


def test_smoke():
    result = parse_track("CARLOS DI SARLI * SENTIMIENTO CRIOLLO * 1941 *")
    assert result['orchestra'] == "CARLOS DI SARLI"
    assert result['track_title'] == "SENTIMIENTO CRIOLLO"
    assert result['year'] == 1941
