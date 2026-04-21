async def test_import() -> None:
    from fidem.environment import Environment

    assert Environment is not None
