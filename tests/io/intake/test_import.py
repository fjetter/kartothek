import intake


def test_entrypoint_set_up():
    assert hasattr(intake, "open_kartothek")
