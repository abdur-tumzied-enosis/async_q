import asyncio
import inspect
import copy
import time
from typing import Optional

from redis.exceptions import RedisError

from . import scripts
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
    get_legacy_keys,
)


def _next_backoff(current: float) -> float:
    return min(current * 2, 30)


async def async_worker(stop_event: Optional[asyncio.Event] = None):
    if not AsyncTaskQueue.get_instance():
        raise RuntimeError('AsyncTaskQueue is not initialized')
    raise NotImplementedError('wired up in Task 7')  # ponytail: temporary, mid-branch only


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

    async def fetch_loop(self) -> None:
        """Pull work only when a concurrency slot is free. Runs until cancelled."""
        backoff = 1.0
        fetched = None  # payload moved to our backup but not yet turned into an item
        while True:
            await self.sem.acquire()
            try:
                fetched = await self.redis.brpoplpush(self.main_key, self.backup_key, timeout=1)
                item = await self.process_message(fetched) if fetched is not None else None
                fetched = None
            except RedisError as e:
                self.sem.release()
                self.logger.warning('redis unavailable (%r); retrying in %.0fs', e, backoff)
                await asyncio.sleep(backoff)
                backoff = _next_backoff(backoff)
                while fetched is not None:
                    # Best effort: hand the stranded payload back to main. If the reply was lost
                    # after the server already moved it, it stays in backup until release() at shutdown.
                    try:
                        await scripts.requeue(self.redis, self.q, self.wid, fetched)
                        fetched = None
                    except RedisError:
                        await asyncio.sleep(backoff)
                        backoff = _next_backoff(backoff)
                continue
            except BaseException:
                self.sem.release()
                raise
            backoff = 1.0
            if item is None:
                self.sem.release()
                continue
            self.start_task(item)

    def start_task(self, item: QueueItem) -> None:
        task = asyncio.create_task(self.run_task(item), name=get_task_key(item.deserialize_data.id))
        self.inflight.add(task)
        task.add_done_callback(lambda t, _item=item: self._on_done(t, _item))

    def _on_done(self, task, item: QueueItem) -> None:
        self.inflight.discard(task)
        bg = asyncio.create_task(self.task_done(task, item))
        self.background.add(bg)
        bg.add_done_callback(self.background.discard)

    async def beat(self) -> None:
        await self.redis.set(self.hb_key, str(time.time()), px=int(self.heartbeat_ttl * 1000))
        # Re-register every beat: a worker wrongly reaped during a network blip comes back.
        await self.redis.sadd(self.workers_key, self.wid)

    async def reap(self) -> None:
        for member in await self.redis.smembers(self.workers_key):
            wid = member.decode() if isinstance(member, bytes) else member
            if wid == self.wid:
                continue
            n = await scripts.reclaim(self.redis, self.q, wid)
            if n > 0:
                self.logger.warning('reclaimed %d task(s) from dead worker %s', n, wid)
            elif n == 0:
                self.logger.info('removed dead worker %s', wid)
        await self.migrate_legacy(startup=False)

    async def promote(self) -> None:
        n = await scripts.promote_due(self.redis, self.q, time.time())
        if n:
            self.logger.debug('promoted %d due retr(y/ies)', n)

    async def migrate_legacy(self, startup: bool) -> int:
        """Move pre-0.3.0 keys into hash-tagged keys. Per item, not atomic across slots.
        # ponytail: delete one release after 0.3.0
        """
        old = get_legacy_keys(self.q)
        pairs = [(old['main'], self.main_key)]
        if startup:
            pairs += [(old['backup'], self.main_key), (old['dead'], self.dead_key)]
        moved = 0
        for src, dst in pairs:
            while True:
                v = await self.redis.rpop(src)
                if v is None:
                    break
                await self.redis.lpush(dst, v)
                moved += 1
                if src == old['backup']:
                    self.logger.warning('requeued legacy in-flight task; assumes no pre-0.3 workers are running')
        while True:
            items = await self.redis.zrange(old['retry'], 0, 99, withscores=True)
            if not items:
                break
            for v, score in items:
                await self.redis.zadd(self.retry_key, {v: score})  # add before remove: crash duplicates, never loses
                await self.redis.zrem(old['retry'], v)
                moved += 1
        if moved:
            self.logger.info('migrated %d item(s) from legacy keys', moved)
        return moved

    async def _every(self, interval: float, fn, name: str) -> None:
        while True:
            try:
                await fn()
            except Exception as e:
                self.logger.warning('%s failed: %r', name, e)
            await asyncio.sleep(interval)
