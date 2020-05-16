import pytest


@pytest.fixture()
def mock_croniter(mocker):
    mock_croniter = mocker.Mock()
    mock_croniter_instance = mocker.Mock(
        get_next=mocker.Mock(return_value=500.00)
    )
    mock_croniter.return_value = mock_croniter_instance
    # mock_croniter.return_value.get_next.return_value = 500.00
    return mock_croniter


@pytest.fixture
def create_mock_coro(mocker, monkeypatch):
    """Fixture to mock coroutines.
    Reference: https://github.com/econchick/mayhem/blob/master/part-5/test_mayhem_3.py
    """

    def _create_mock_patch_coro(to_patch=None):
        mock = mocker.Mock()

        async def _coro(*args, **kwargs):
            return mock(*args, **kwargs)

        if to_patch:  # <-- may not need/want to patch anything
            monkeypatch.setattr(to_patch, _coro)
        return mock, _coro

    return _create_mock_patch_coro
