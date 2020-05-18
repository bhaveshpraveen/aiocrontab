import logging

import pytest
from aiocrontab import Crontab
from aiocrontab.core import Task


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


@pytest.fixture
def crontab():
    def _crontab(*args, **kwargs):
        return Crontab(*args, **kwargs)

    return _crontab


@pytest.fixture
def create_caplog(caplog):
    """Set global test logging levels."""

    def _level(level=logging.INFO):
        caplog.set_level(level)
        return caplog

    return _level


@pytest.fixture
def mock_crontab_with_tasks(mocker, crontab):
    def _crontab(*args, **kwargs):
        should_mock_handle_cron_job = kwargs.pop("mock_handle_cronjob", False)
        cron = crontab(*args, **kwargs)

        mocker.patch(
            "aiocrontab.core.Task.time",
            mocker.Mock(timestamp=mocker.Mock(return_value=0.01)),
        )
        mocker.patch("aiocrontab.core.Task.next_timestamp", 0.05)
        mocker.patch("aiocrontab.core.Task.next_loop_timestamp", 0.03)

        f_returns = [
            None,
            None,
            Exception("Exception Raised"),
            None,
            Exception("Exception Raised"),
        ]

        f1 = mocker.Mock(side_effect=f_returns)

        if should_mock_handle_cron_job:

            async def mock_handle_cronjob(*args, **kwargs):
                for i in range(len(f_returns)):
                    task = Task(*args, **kwargs)
                    await task.complete_task_lifecycle()

                await cron.shutdown()

            mocker.patch("aiocrontab.core.handle_cronjob", mock_handle_cronjob)

        cron.register("* * * * *")(f1)
        return cron, f1

    return _crontab
