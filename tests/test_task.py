import asyncio
import logging

from datetime import datetime
from datetime import timezone

import pytest

import aiocrontab.core

from aiocrontab.core import Task
from aiocrontab.core import create_logger


@pytest.mark.parametrize(
    "now,pattern,expected",
    [
        (
            datetime(2020, 5, 5, 0, 0, 0),
            "* * * * *",
            datetime(2020, 5, 5, 0, 1, 0),
        ),
        (
            datetime(2020, 5, 5, 0, 0, 0),
            "*/5 * * * *",
            datetime(2020, 5, 5, 0, 5, 0),
        ),
        (
            datetime(2020, 5, 5, 0, 0, 0),
            "55 11 6 5 *",
            datetime(2020, 5, 6, 11, 55, 0),
        ),
    ],
)
def test_get_next_returns_task_datetime_and_current_datetime(
    now, pattern, expected, mocker
):
    task = Task(
        pattern=pattern,
        func=mocker.Mock(),
        loop=mocker.Mock(),
        executor=mocker.Mock(),
        logger=mocker.Mock(),
        tz=timezone.utc,
    )
    task.get_now = mocker.Mock(return_value=now)

    task_datetime, _now = task.get_next()

    assert _now == now
    assert task_datetime == expected


def test_get_now_returns_timezone_aware_datetime(mocker):
    task = Task(
        pattern="* * * * *",
        func=mocker.Mock(),
        loop=mocker.Mock(),
        executor=mocker.Mock(),
        logger=mocker.Mock(),
        tz=timezone.utc,
    )
    d = task.get_now()
    # how to check if the dt is timezone aware:
    # https://stackoverflow.com/questions/5802108/how-to-check-if-a-datetime-object-is-localized-with-pytz
    assert d.tzinfo is not None and d.tzinfo.utcoffset(d) is not None


@pytest.mark.parametrize(
    "now,dt,timestamp",
    [
        (datetime(2020, 5, 5, 0, 0, 0), datetime(2020, 5, 5, 0, 1, 0), 60.0),
        (
            datetime(2020, 5, 5, 0, 0, 0),
            datetime(2020, 5, 6, 12, 12, 12),
            130332.0,
        ),
    ],
)
@pytest.mark.asyncio
async def test_sleep_until_task_completion(
    now, dt, timestamp, event_loop, mocker, create_mock_coro, create_caplog
):
    caplog = create_caplog(logging.DEBUG)
    task = Task(
        pattern="* * * * *",
        func=mocker.Mock(),
        loop=event_loop,
        executor=mocker.Mock(),
        logger=create_logger(),
        tz=timezone.utc,
    )
    task.get_now = mocker.Mock(return_value=now)
    mock, coro = create_mock_coro("aiocrontab.core.asyncio.sleep")
    await task.sleep_until_task_completion(till=dt)

    mock.assert_called_once_with(timestamp + task.buffer_time)
    assert 2 == len(caplog.records)


@pytest.mark.parametrize(
    "pattern,now,dt",
    [
        (
            "* * * * *",
            datetime(2020, 5, 5, 0, 0, 0),
            datetime(2020, 5, 5, 0, 1, 0),
        ),
        (
            "3 1-4 * * *",
            datetime(2020, 5, 5, 0, 0),
            datetime(2020, 5, 5, 1, 2, 0),
        ),
    ],
)
@pytest.mark.asyncio
async def test_scheduled_time_is_less_than_sleep_time(
    pattern, now, dt, mocker, event_loop, create_mock_coro
):
    task = Task(
        pattern=pattern,
        func=mocker.Mock(),
        loop=event_loop,
        executor=mocker.Mock(),
        logger=mocker.Mock(),
        tz=timezone.utc,
    )
    # mocks
    task.get_now = mocker.Mock(return_value=now)
    task.get_next = mocker.Mock(return_value=(dt, now))
    task.loop.call_at = mocker.Mock()
    task.loop.time = mocker.Mock(return_value=0.0)
    mock_sleep, _ = create_mock_coro("aiocrontab.core.asyncio.sleep")

    # run
    await task.complete_task_lifecycle()

    # asserts

    assert mock_sleep.call_args[0][0] > task.loop.call_at.call_args[0][0]
    assert (
        mock_sleep.call_args[0][0] - task.loop.call_at.call_args[0][0]
        == task.buffer_time
    )


@pytest.mark.parametrize(
    "at,now,loop_time,expected",
    [
        (
            datetime(2020, 5, 5, 0, 2, 0),
            datetime(2020, 5, 5, 0, 0, 0),
            30.0,
            150.0,
        ),
        (
            datetime(2020, 5, 7, 13, 12, 0),
            datetime(2020, 5, 5, 0, 0, 0),
            23.0,
            220343.0,
        ),
        (
            datetime(2020, 6, 1, 0, 3, 4),
            datetime(2020, 5, 5, 0, 0, 0),
            139.0,
            2333123.0,
        ),
    ],
)
def test_schedule_next_loop_timestamp_is_calculated_correctly(
    at, now, expected, loop_time, mocker
):
    task = Task(
        pattern="* * * * *",
        func=mocker.Mock(),
        loop=mocker.Mock(run_in_executor=mocker.Mock()),
        executor=mocker.Mock(),
        logger=mocker.Mock(),
        tz=timezone.utc,
    )

    # mock
    task.loop.time = mocker.Mock(return_value=loop_time)
    task.loop.call_at = mocker.Mock()

    task.schedule(at, now)

    # asserts
    task.loop.call_at.assert_called_once_with(expected, task.run)


def test_run_logs_messages(mocker, create_caplog, create_mock_coro):
    task1 = Task(
        pattern="* * * * *",
        func=mocker.Mock(),
        loop=mocker.Mock(run_in_executor=mocker.Mock()),
        executor=mocker.Mock(),
        logger=create_logger(),
        tz=timezone.utc,
    )
    caplog = create_caplog(logging.DEBUG)
    task1.run()

    assert len(caplog.records) == 1
    assert "Scheduling func: unknown in a Thread." == caplog.records[0].msg
    caplog.clear()
    assert len(caplog.records) == 0

    mock, coro = create_mock_coro()
    task2 = Task(
        pattern="* * * * *",
        func=coro,
        loop=mocker.Mock(run_in_executor=mocker.Mock()),
        executor=mocker.Mock(),
        logger=create_logger(),
        tz=timezone.utc,
    )
    task2.run()

    assert len(caplog.records) == 1
    assert "Scheduling func: _coro in the Event Loop." == caplog.records[0].msg


@pytest.mark.parametrize(
    "at,now", [(datetime(2020, 5, 5, 0, 0, 1), datetime(2020, 5, 5, 0, 0, 0))]
)
@pytest.mark.asyncio
async def test_run_gets_called_from_the_schedule_call(
    at, now, mocker, event_loop
):
    task = Task(
        pattern="dummy",
        func=mocker.Mock(),
        loop=event_loop,
        executor=mocker.Mock(),
        logger=mocker.Mock(),
        tz=timezone.utc,
    )
    task.run = mocker.Mock()

    task.schedule(at, now)

    await asyncio.sleep(1.2)

    task.run.assert_called_once_with()


@pytest.mark.asyncio
async def test_handle_cronjob(mocker, create_mock_coro):
    _, mock_coro = create_mock_coro()
    mock_complete_task_lifecycle = mocker.Mock()
    mock_complete_task_lifecycle.side_effect = [
        mock_coro(),
        Exception("Something went wrong."),
    ]
    mock_task_class = mocker.Mock(
        return_value=mocker.Mock(
            complete_task_lifecycle=mock_complete_task_lifecycle
        )
    )
    mocker.patch("aiocrontab.core.Task", mock_task_class)

    mock_func = mocker.Mock()
    # aiocrontab.core.handle_cronjob("* * * * *", mock_func, loop=mocker.Mock(), executor=mocker.Mock())

    with pytest.raises(Exception, match="Something went wrong."):
        await aiocrontab.core.handle_cronjob(
            "* * * * *",
            mock_func,
            loop=mocker.Mock(),
            executor=mocker.Mock(),
            logger=mocker.Mock(),
            tz=timezone.utc,
        )

    assert mock_complete_task_lifecycle.call_count == 2
    assert mock_task_class.call_count == 2


@pytest.mark.asyncio
async def test_sync_task_is_scheduled_in_thread(mocker, event_loop):
    # mock
    task = Task(
        pattern="* * * * *",
        func=mocker.Mock(),
        loop=event_loop,
        executor=mocker.Mock(),
        logger=mocker.Mock(),
        tz=timezone.utc,
    )
    task.buffer_time = 0.1
    task.loop.run_in_executor = mocker.Mock()

    # run code
    task.schedule(
        at=datetime(2020, 5, 5, 0, 0, 0, 30),
        now=datetime(2020, 5, 5, 0, 0, 0, 0),
    )

    # wait till the tasks gets executed
    await asyncio.sleep(0.5)

    # assert
    task.loop.run_in_executor.assert_called_once_with(task.executor, task.func)


@pytest.mark.asyncio
async def test_async_task_is_scheduled_in_event_loop(
    mocker, event_loop, create_mock_coro
):
    mock, coro = create_mock_coro()
    task = Task(
        pattern="* * * * *",
        func=coro,
        loop=event_loop,
        executor=mocker.Mock(),
        logger=mocker.Mock(),
        tz=timezone.utc,
    )
    task.buffer_time = 0.1
    task.loop.create_task = mocker.Mock(wraps=task.loop.create_task)
    # run code
    task.schedule(
        at=datetime(2020, 5, 5, 0, 0, 0, 30),
        now=datetime(2020, 5, 5, 0, 0, 0, 0),
    )

    await asyncio.sleep(0.5)

    assert task.loop.create_task.call_count == 1

    # to check if the coroutine was actually run
    mock.assert_called_once_with()
