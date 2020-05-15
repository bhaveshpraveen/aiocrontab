from datetime import datetime

import aiocrontab.task
from aiocrontab.task import Task


def test_time_functions_properly(mocker):
    task = Task(
        pattern="",
        func=mocker.Mock(),
        loop=mocker.Mock(),
        executor=mocker.Mock(),
    )
    assert task._time is None

    # time is now set
    now = task.time
    assert task._time == now
    assert isinstance(now, datetime)

    # the time is the same on subsequent calls when it was first set
    second_call = task.time
    assert now == second_call
    assert task._time == now


def test_next_timestamp(mocker, mock_croniter):
    task = Task(
        pattern="",
        func=mocker.Mock(),
        loop=mocker.Mock(),
        executor=mocker.Mock(),
    )

    mocker.patch("aiocrontab.task.croniter", mock_croniter)
    assert task._next_timestamp is None

    # next_timestamp is now set
    timestamp_now = task.next_timestamp
    assert timestamp_now == 500.00
    assert isinstance(timestamp_now, float)

    # next_timestamp is the same on subsequent calls
    timestamp_latest = task.next_timestamp
    assert isinstance(timestamp_latest, float)
    assert timestamp_now == timestamp_latest

    t1 = task.time
    aiocrontab.task.croniter.assert_called_once_with("", t1)
    aiocrontab.task.croniter.return_value.get_next.assert_called_once_with(
        ret_type=float
    )


def test_next_loop_timestamp(mocker):
    task = Task(
        pattern="",
        func=mocker.Mock(),
        loop=mocker.Mock(time=mocker.Mock(return_value=1.0)),
        executor=mocker.Mock(),
    )

    task._time = mocker.Mock(timestamp=mocker.Mock(return_value=0.0))
    task._next_timestamp = 0.0

    # next_loop_timstamp is not set
    assert task._next_loop_timestamp is None

    # next_loop_timestamp is set
    next_loop_timestamp_now = task.next_loop_timestamp
    assert isinstance(next_loop_timestamp_now, float)
    assert next_loop_timestamp_now == 1.0
    assert task.loop.time.call_count == 1

    # next_loop_timestamp is the same on subsequent calls
    next_loop_timestamp_latest = task.next_loop_timestamp
    assert next_loop_timestamp_now == 1.0
    assert next_loop_timestamp_latest == 1.0
    assert task.loop.time.call_count == 1


def test_sleep_until_task_completion(event_loop, mocker):
    Task(
        pattern="", func=mocker.Mock(), loop=event_loop, executor=mocker.Mock()
    )
