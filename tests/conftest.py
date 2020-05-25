import logging

from datetime import datetime

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
        caplog.set_level(level, logger="aiocrontab.core")
        return caplog

    return _level


@pytest.fixture
def mock_crontab_with_tasks(mocker, crontab):
    def _crontab(*args, **kwargs):
        should_mock_handle_cron_job = kwargs.pop("mock_handle_cronjob", False)
        cron = crontab(*args, **kwargs)

        mocker.patch(
            "aiocrontab.core.Task.get_now",
            mocker.Mock(return_value=datetime(2020, 5, 5, 0, 0, 0)),
        )
        mocker.patch(
            "aiocrontab.core.Task.get_next",
            mocker.Mock(
                return_value=(
                    datetime(2020, 5, 5, 0, 0, 0, 30),
                    datetime(2020, 5, 5, 0, 0, 0, 0),
                )
            ),
        )

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
                    # important: reduce the buffer time for testing
                    task.buffer_time = 0.1
                    await task.complete_task_lifecycle()

                await cron.shutdown()

        else:

            async def mock_handle_cronjob(*args, **kwargs):
                while True:
                    task = Task(*args, **kwargs)
                    # important: reduce the buffer time for testing
                    task.buffer_time = 0.1
                    await task.complete_task_lifecycle()

        mocker.patch("aiocrontab.core.handle_cronjob", mock_handle_cronjob)

        cron.register("* * * * *")(f1)
        return cron, f1

    return _crontab


@pytest.fixture
def create_mock_handle_cronjob(mocker):
    def dec(to_patch=None):
        async def _mock_handle_cronjob(pattern, func, loop, executor, logger):
            while True:
                task = Task(
                    pattern,
                    func=func,
                    loop=loop,
                    executor=executor,
                    logger=logger,
                )
                task.buffer_time = 0.1
                await task.complete_task_lifecycle()

        mock = mocker.Mock(wraps=_mock_handle_cronjob)

        if to_patch:
            mocker.patch(to_patch, mock)

        return mock

    return dec
