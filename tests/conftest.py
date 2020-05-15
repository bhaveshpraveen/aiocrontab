import pytest


@pytest.fixture()
def mock_croniter(mocker):
    mock_croniter = mocker.Mock()
    mock_croniter_instance = mocker.Mock(get_next=mocker.Mock(return_value=500.00))
    mock_croniter.return_value = mock_croniter_instance
    # mock_croniter.return_value.get_next.return_value = 500.00
    return mock_croniter

