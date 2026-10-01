import asyncio
import time

import pytest

from fakes import FakeAsyncRedis, FakeTask, NoopLogger
from async_q.async_worker import _task_done
from async_q.utils import (
    TaskMetaData,
    QueueItem,
    serialize,
    get_redis_q_key,
    get_redis_q_backup_key,
    get_task_key,
    get_retry_zset_key,
    get_dead_letter_key,
)


@pytest.mark.asyncio
async def test_task_done_schedules_retry_when_under_limit():
    redis = FakeAsyncRedis()
    qname = 'default'
    backup_key = get_redis_q_backup_key(qname)
    retry_key = get_retry_zset_key(qname)

    meta = TaskMetaData(
        id='tid1',
        path='/tmp/x.py',
        func_name='foo',
        args=[],
        kwargs={},
        status='starting',
        attempt=0,
        max_retries=3,
        backoff_base=0.1,
        backoff_factor=2.0,
        backoff_max=1.0,
    )
    payload = serialize(meta)
    # simulate it currently in backup
    await redis.lpush(backup_key, payload)

    item = QueueItem(func_ref=None, deserialize_data=meta, original_data=payload)

    # Simulate task failure
    task = FakeTask(exc=RuntimeError('boom'))

    class DummyQ:
        def task_done(self):
            pass

    await _task_done(task, DummyQ(), qname, redis, logger=NoopLogger(), item=item)

    # Should be scheduled for retry
    assert retry_key in redis.zsets
    assert payload in redis.zsets[retry_key]
    # removed from backup
    assert payload not in redis.lists.get(backup_key, [])


@pytest.mark.asyncio
async def test_task_done_moves_to_dlq_when_exhausted():
    redis = FakeAsyncRedis()
    qname = 'default'
    backup_key = get_redis_q_backup_key(qname)
    dlq_key = get_dead_letter_key(qname)

    meta = TaskMetaData(
        id='tid2',
        path='/tmp/x.py',
        func_name='foo',
        args=[],
        kwargs={},
        status='starting',
        attempt=3,
        max_retries=3,
    )
    payload = serialize(meta)
    await redis.lpush(backup_key, payload)

    item = QueueItem(func_ref=None, deserialize_data=meta, original_data=payload)
    task = FakeTask(exc=RuntimeError('boom'))

    class DummyQ:
        def task_done(self):
            pass

    await _task_done(task, DummyQ(), qname, redis, logger=NoopLogger(), item=item)

    # Should be in DLQ
    assert dlq_key in redis.lists
    assert redis.lists[dlq_key][0] == payload
    # removed from backup
    assert payload not in redis.lists.get(backup_key, [])
