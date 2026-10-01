import asyncio
import inspect
import logging
import re
import traceback
import copy
import time
from typing import Any, Dict, Optional

import redis.asyncio as aioredis

from .async_q import AsyncTaskQueue
from .utils import (
    QueueItem,
    TaskMetaData,
    deserialize,
    get_function_ref,
    serialize,
    get_redis_q_key,
    get_redis_q_backup_key,
    get_task_key,
    to_thread,
    get_retry_zset_key,
    get_dead_letter_key,
    get_heartbeat_key,
    get_workers_key,
    new_worker_id,
)


class AsyncQueue(asyncio.Queue):

    def full(self) -> bool:
        isFull = super().full()
        async_q_task = [t.get_name() for t in asyncio.all_tasks(
        ) if re.match(get_task_key('.*'), t.get_name())]
        logging.debug(f'async_q_task count {len(async_q_task)}')
        logging.debug(
            f'AsyncQueue is Full  -> {isFull or len(async_q_task) >= self.maxsize}')
        return isFull or len(async_q_task) >= self.maxsize

    def task_done(self) -> None:
        super().task_done()
        self._wakeup_next(self._putters)

    def get_nowait(self):
        """Remove and return an item from the queue.

        Return an item if one is immediately available, else raise QueueEmpty.
        """
        if self.empty():
            raise asyncio.QueueEmpty
        item = self._get()
        return item

async def _task_done(task: asyncio.Task, queue: AsyncQueue, distribute_qname: str, redis: aioredis.Redis, logger, item: QueueItem):
    try:
        # Success path returns the same item
        returned_item: QueueItem = task.result()
        data = copy.deepcopy(returned_item.deserialize_data)
        logger.debug(
            f"removing {returned_item.original_data} from {get_redis_q_backup_key(distribute_qname)}")
        r = await redis.lrem(get_redis_q_backup_key(distribute_qname), count=0, value=returned_item.original_data)
        logger.debug(f"removed item from backup queue & status is : {r}")
        data.status = 'finished'
        await redis.set(get_task_key(data.id), serialize(data), ex=60 * 60)
    except asyncio.CancelledError:
        # Requeue the item for future processing
        logger.info("task cancelled, re-queueing item")
        meta = copy.deepcopy(item.deserialize_data)
        meta.status = 'submitted'
        await redis.set(get_task_key(meta.id), serialize(meta))
        # Move from backup to main (remove specific value then LPUSH to main)
        await redis.lrem(get_redis_q_backup_key(distribute_qname), count=1, value=item.original_data)
        await redis.lpush(get_redis_q_key(distribute_qname), item.original_data)
    except Exception as e:
        # Failure: compute retry or DLQ
        logger.error("task failed: %s", e)
        logger.debug('%s', traceback.format_exc())
        meta = copy.deepcopy(item.deserialize_data)
        meta.status = 'failed'
        # Remove from backup queue
        await redis.lrem(get_redis_q_backup_key(distribute_qname), count=1, value=item.original_data)

        if meta.attempt < meta.max_retries:
            meta.attempt += 1
            # Exponential backoff with jitter can be added; keep simple deterministic for now
            delay = min(meta.backoff_base * (meta.backoff_factor ** (meta.attempt - 1)), meta.backoff_max)
            import time
            score = time.time() + delay
            meta.next_retry_at = score
            await redis.set(get_task_key(meta.id), serialize(meta))
            # Re-serialize so the bumped attempt count travels with the payload
            await redis.zadd(get_retry_zset_key(distribute_qname), {serialize(meta): score})
            logger.info('scheduled retry #%s in %.2fs for %s', meta.attempt, delay, meta.id)
        else:
            # Send to DLQ
            await redis.set(get_task_key(meta.id), serialize(meta))
            await redis.lpush(get_dead_letter_key(distribute_qname), serialize(meta))
            logger.warning('moved task %s to DLQ after %s attempts', meta.id, meta.attempt)
    finally:
        queue.task_done()

# Strong refs so the event loop can't GC done-handlers mid-flight
_background_tasks: set = set()


def task_done_callback(task, queue: AsyncQueue, distribute_qname: str, redis: aioredis.Redis, logger, item: QueueItem):
    t = asyncio.create_task(_task_done(task, queue, distribute_qname, redis, logger, item))
    _background_tasks.add(t)
    t.add_done_callback(_background_tasks.discard)


async def _task_inner(tkey, func, item: QueueItem, redis, logger):
    meta_data = copy.deepcopy(item.deserialize_data)
    meta_data.status = 'starting'
    await redis.set(tkey, serialize(meta_data))
    logger.debug(f'starting a new task; task_id: {tkey}')
    coro = func(*meta_data.args, **{**meta_data.kwargs, 'task_id': tkey})
    await coro
    return item

async def create_async_task_from_queue(distribute_qname: str, redis: aioredis.Redis, queue: AsyncQueue, logger, stop_event: asyncio.Event):
    while True:
        # Try to get a unit of work with a short timeout so we can react to stop_event
        if stop_event.is_set() and queue.empty():
            break
        try:
            item: QueueItem = await asyncio.wait_for(queue.get(), timeout=0.5)
        except asyncio.TimeoutError:
            continue

        func_ref = item.func_ref
        task_key = get_task_key(item.deserialize_data.id)


        logger.info(f'creating a new task: {task_key}')
        task = asyncio.create_task(
            coro=_task_inner(task_key, func_ref, item,redis, logger), name=task_key)

        task.add_done_callback(lambda t, _item=item: task_done_callback(t, queue, distribute_qname, redis, logger, _item))


async def _reject(value, distribute_qname: str, redis: aioredis.Redis, logger, reason: str, meta: Optional[TaskMetaData] = None):
    """Move an unprocessable payload from backup to the DLQ so it can't crash the worker again."""
    logger.error('rejecting task to DLQ: %s', reason)
    await redis.lrem(get_redis_q_backup_key(distribute_qname), count=1, value=value)
    await redis.lpush(get_dead_letter_key(distribute_qname), value)
    if meta is not None:
        meta.status = 'failed'
        await redis.set(get_task_key(meta.id), serialize(meta))


async def _process_message(value, distribute_qname: str, redis: aioredis.Redis, queue: asyncio.Queue, logger):
    try:
        data = deserialize(value)
    except Exception as e:
        await _reject(value, distribute_qname, redis, logger, f'undecodable payload: {e!r}')
        return

    logger.debug("deserialize received value : %s", data)

    try:
        fun = await to_thread(get_function_ref, data.path, data.func_name)
    except Exception as e:
        await _reject(value, distribute_qname, redis, logger, f'cannot load {data.path}:{data.func_name}: {e!r}', data)
        return

    if not inspect.iscoroutinefunction(fun):
        await _reject(value, distribute_qname, redis, logger, f'{data.func_name} is not a coroutine function', data)
        return

    data.status = 'pending'
    await redis.set(get_task_key(data.id), serialize(data))
    await queue.put(QueueItem(func_ref=fun, deserialize_data=data, original_data=value))


async def listen_to_submitted_task(distribute_qname: str, redis: aioredis.Redis, queue: AsyncQueue, logger, stop_event: asyncio.Event):

    # get the unfinished task form the backup queue
    while True:
        value = await redis.rpoplpush(get_redis_q_backup_key(distribute_qname), get_redis_q_key(distribute_qname))
        if not value:
            break

    while not stop_event.is_set():
        value = await redis.brpoplpush(
            get_redis_q_key(distribute_qname),
            get_redis_q_backup_key(distribute_qname),
            timeout=1,
        )

        if value is None:
            # Also check retry ZSET for due items
            import time
            now = time.time()
            try:
                # Fetch one due item
                due = await redis.zrangebyscore(get_retry_zset_key(distribute_qname), min=-1, max=now, start=0, num=1)
                if due:
                    payload = due[0]
                    # Move to backup and remove from ZSET
                    await redis.zrem(get_retry_zset_key(distribute_qname), payload)
                    await redis.lpush(get_redis_q_key(distribute_qname), payload)
                    # Continue loop to process as normal
            except Exception:
                pass
            continue

        await _process_message(value, distribute_qname, redis, queue, logger)


async def async_worker(stop_event: Optional[asyncio.Event] = None):
    if not AsyncTaskQueue.get_instance():
        raise RuntimeError('AsyncTaskQueue is not initialized')

    async_task_q = AsyncTaskQueue.get_instance()
    r = async_task_q.redis_builder.get_redis_async()
    queue = AsyncQueue(maxsize=async_task_q.get_concurrency())
    if stop_event is None:
        stop_event = asyncio.Event()
    try:
        p_task = None
        c_task = None

        producer = listen_to_submitted_task(
            async_task_q.distribute_qname, r, queue, async_task_q.logger, stop_event)
        consumer = create_async_task_from_queue(
            async_task_q.distribute_qname, r, queue, async_task_q.logger, stop_event)

        p_task = asyncio.create_task(producer)
        c_task = asyncio.create_task(consumer)

        await asyncio.gather(p_task, c_task)

    except Exception as e:
        async_task_q.logger.error(e)
        async_task_q.logger.debug('%s', traceback.format_exc())
    finally:
        # Safely cancel tasks if they were created
        try:
            if p_task is not None:
                p_task.cancel()
            if c_task is not None:
                c_task.cancel()
        except Exception:
            pass

        # Requeue any remaining items in backup back to main queue and set status to submitted
        try:
            while True:
                moved = await r.rpoplpush(
                    get_redis_q_backup_key(async_task_q.distribute_qname),
                    get_redis_q_key(async_task_q.distribute_qname),
                )
                if not moved:
                    break
                try:
                    meta = deserialize(moved)
                    meta.status = 'submitted'
                    # When shutting down, retry attempts remain as-is
                    await r.set(get_task_key(meta.id), serialize(meta))
                except Exception:
                    # best effort
                    pass
        except Exception:
            pass

        await r.close()


class Worker:
    """One worker process on one queue. Construct inside a running event loop."""

    def __init__(self, redis, queue_name: str, concurrency: int, logger, *,
                 status_ttl: int = 86400, heartbeat_interval: float = 10.0,
                 heartbeat_ttl: float = 30.0, shutdown_timeout: float = 30.0,
                 wid: Optional[str] = None):
        self.redis = redis
        self.q = queue_name
        self.logger = logger
        self.status_ttl = status_ttl
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_ttl = heartbeat_ttl
        self.shutdown_timeout = shutdown_timeout
        self.wid = wid or new_worker_id()
        self.main_key = get_redis_q_key(queue_name)
        self.backup_key = get_redis_q_backup_key(queue_name, self.wid)
        self.hb_key = get_heartbeat_key(queue_name, self.wid)
        self.workers_key = get_workers_key(queue_name)
        self.retry_key = get_retry_zset_key(queue_name)
        self.dead_key = get_dead_letter_key(queue_name)
        self.sem = asyncio.Semaphore(concurrency)
        self.inflight: set = set()
        self.background: set = set()  # strong refs: done-handlers must not be GC'd mid-flight

    async def set_status(self, meta: TaskMetaData, status: str) -> None:
        meta.status = status
        await self.redis.set(get_task_key(meta.id), serialize(meta), ex=self.status_ttl)

    async def reject(self, value, reason: str, meta: Optional[TaskMetaData] = None) -> None:
        """Move an unprocessable payload from backup to the DLQ so it can't crash the worker again."""
        self.logger.error('rejecting task to DLQ: %s', reason)
        await self.redis.lrem(self.backup_key, 1, value)
        await self.redis.lpush(self.dead_key, value)
        if meta is not None:
            await self.set_status(meta, 'failed')

    async def process_message(self, value) -> Optional[QueueItem]:
        try:
            data = deserialize(value)
        except Exception as e:
            await self.reject(value, f'undecodable payload: {e!r}')
            return None

        self.logger.debug("deserialize received value : %s", data)

        try:
            fun = await to_thread(get_function_ref, data.path, data.func_name)
        except Exception as e:
            await self.reject(value, f'cannot load {data.path}:{data.func_name}: {e!r}', data)
            return None

        if not inspect.iscoroutinefunction(fun):
            await self.reject(value, f'{data.func_name} is not a coroutine function', data)
            return None

        await self.set_status(data, 'pending')
        return QueueItem(func_ref=fun, deserialize_data=data, original_data=value)

    async def run_task(self, item: QueueItem) -> QueueItem:
        meta = copy.deepcopy(item.deserialize_data)
        await self.set_status(meta, 'starting')
        tkey = get_task_key(meta.id)
        self.logger.debug('starting a new task; task_id: %s', tkey)
        await item.func_ref(*meta.args, **{**meta.kwargs, 'task_id': tkey})
        return item

    async def task_done(self, task, item: QueueItem) -> None:
        meta = copy.deepcopy(item.deserialize_data)
        try:
            try:
                task.result()
            except asyncio.CancelledError:
                # Front of the queue: it was already running, re-run it first.
                if await self.redis.lrem(self.backup_key, 1, item.original_data):
                    await self.redis.rpush(self.main_key, item.original_data)
                await self.set_status(meta, 'submitted')
                self.logger.info('task %s cancelled; re-queued', meta.id)
                return
            except Exception as e:
                self.logger.error('task %s failed: %r', meta.id, e)
                self.logger.debug('traceback', exc_info=e)
                if not await self._ack(item, meta):
                    return
                if meta.attempt < meta.max_retries:
                    meta.attempt += 1
                    delay = min(meta.backoff_base * (meta.backoff_factor ** (meta.attempt - 1)), meta.backoff_max)
                    meta.next_retry_at = time.time() + delay
                    await self.set_status(meta, 'failed')
                    # Re-serialize so the bumped attempt count travels with the payload
                    await self.redis.zadd(self.retry_key, {serialize(meta): meta.next_retry_at})
                    self.logger.info('scheduled retry #%s in %.2fs for %s', meta.attempt, delay, meta.id)
                else:
                    await self.set_status(meta, 'failed')
                    await self.redis.lpush(self.dead_key, serialize(meta))
                    self.logger.warning('moved task %s to DLQ after %s attempts', meta.id, meta.attempt)
                return
            if await self._ack(item, meta):
                await self.set_status(meta, 'finished')
        finally:
            self.sem.release()

    async def _ack(self, item: QueueItem, meta: TaskMetaData) -> bool:
        """Remove from own backup. False means a peer reclaimed it (task may run twice)."""
        if await self.redis.lrem(self.backup_key, 1, item.original_data):
            return True
        self.logger.warning('task %s was reclaimed by another worker while running; it may run twice', meta.id)
        return False
