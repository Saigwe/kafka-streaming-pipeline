from stream.windows import window_start


def test_window_start_is_aligned():
    assert window_start(1_000_123, 300) == 1_000_123 - (1_000_123 % 300)
    assert window_start(600, 300) == 600
