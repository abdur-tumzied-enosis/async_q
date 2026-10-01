"""Atomic multi-key queue operations as Lua scripts.

Every key a script touches belongs to one queue and carries the same {q} hash
tag, so each script runs in a single Redis Cluster slot.
"""
from .utils import (
    get_heartbeat_key,
    get_redis_q_backup_key,
    get_redis_q_key,
    get_retry_zset_key,
    get_workers_key,
)

# KEYS: hb, backup, main, registry  ARGV: wid
_RECLAIM = """
if redis.call('EXISTS', KEYS[1]) == 1 then return -1 end
local n = 0
while redis.call('RPOPLPUSH', KEYS[2], KEYS[3]) do n = n + 1 end
redis.call('SREM', KEYS[4], ARGV[1])
return n
"""

# KEYS: retry, main  ARGV: now, limit
_PROMOTE_DUE = """
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, v in ipairs(due) do
  redis.call('ZREM', KEYS[1], v)
  redis.call('RPUSH', KEYS[2], v)
end
return #due
"""

# KEYS: backup, main, hb, registry  ARGV: wid
_RELEASE = """
local n = 0
while redis.call('RPOPLPUSH', KEYS[1], KEYS[2]) do n = n + 1 end
redis.call('DEL', KEYS[3])
redis.call('SREM', KEYS[4], ARGV[1])
return n
"""


async def reclaim(redis, q: str, wid: str) -> int:
    """Return a dead worker's in-flight payloads to the main queue; -1 if it is alive."""
    return await redis.eval(
        _RECLAIM, 4,
        get_heartbeat_key(q, wid), get_redis_q_backup_key(q, wid), get_redis_q_key(q), get_workers_key(q),
        wid,
    )


async def promote_due(redis, q: str, now: float, limit: int = 100) -> int:
    """Move due retries to the front of the main queue."""
    return await redis.eval(_PROMOTE_DUE, 2, get_retry_zset_key(q), get_redis_q_key(q), now, limit)


async def release(redis, q: str, wid: str) -> int:
    """Graceful exit: requeue own backup, drop heartbeat and registry entry."""
    return await redis.eval(
        _RELEASE, 4,
        get_redis_q_backup_key(q, wid), get_redis_q_key(q), get_heartbeat_key(q, wid), get_workers_key(q),
        wid,
    )
