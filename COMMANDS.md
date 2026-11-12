# Redis Clone — Supported Commands

All commands follow standard Redis syntax. Connect via the interactive client:
```
java -cp out com.redis.stress.InteractiveClient
```

---

## General

### PING
Check if the server is alive.
```
PING              → PONG
PING "hello"      → "hello"
```

---

## String Commands

Strings are the most basic type. Any key that isn't a list/hash/set/zset is a string.

### SET
Store a value. Supports optional flags.
```
SET key value
SET key value EX 60          # expire in 60 seconds
SET key value PX 5000        # expire in 5000 milliseconds
SET key value NX             # only set if key does NOT exist
SET key value XX             # only set if key ALREADY exists
SET key value EX 30 NX       # combine: only if absent, expire in 30s
```
Returns `OK` on success, `(nil)` if NX/XX condition not met.

### GET
Retrieve a value.
```
GET key          → "value" or (nil) if not found / expired
```

### DEL
Delete one or more keys.
```
DEL key1 key2 key3     → (integer) number of keys actually deleted
```

### MSET
Set multiple keys at once (atomic).
```
MSET name Mohan city Hyderabad age 25    → OK
```

### MGET
Get multiple values at once.
```
MGET name city age
→ 1) "Mohan"
  2) "Hyderabad"
  3) "25"
```
Returns `(nil)` for any key that doesn't exist.

### INCR
Increment a numeric string value by 1. Creates the key at 0 first if it doesn't exist.
```
SET counter 10
INCR counter          → (integer) 11
INCR counter          → (integer) 12
INCR newkey           → (integer) 1
```

### DECR
Decrement a numeric string value by 1.
```
SET counter 10
DECR counter          → (integer) 9
```

### APPEND
Append a string to the end of an existing value. Creates the key if absent.
```
SET greeting "Hello"
APPEND greeting " World"    → (integer) 11   ← new length
GET greeting                → "Hello World"
```

### STRLEN
Get the length of a string value.
```
SET name "Mohan"
STRLEN name            → (integer) 5
STRLEN nokey           → (integer) 0
```

### SETNX
Set a key only if it does NOT already exist. (Legacy — prefer `SET key val NX`)
```
SETNX newkey "value"    → (integer) 1  ← set
SETNX newkey "other"    → (integer) 0  ← not set, already exists
```

### GETSET
Set a new value and return the OLD value atomically.
```
SET name "Mohan"
GETSET name "Chaitanya"    → "Mohan"
GET name                   → "Chaitanya"
```

### GETDEL
Get the value and delete the key atomically.
```
SET temp "abc"
GETDEL temp    → "abc"
GET temp       → (nil)
```

### GETEX
Get the value and optionally update its expiry at the same time.
```
GETEX key EX 60        # get + set expire 60s
GETEX key PX 5000      # get + set expire 5000ms
GETEX key EXAT 1700000000  # get + set absolute unix timestamp (s)
GETEX key PXAT 1700000000000  # get + set absolute unix timestamp (ms)
GETEX key PERSIST      # get + remove any existing expiry
```

---

## TTL / Expiry Commands

### EXPIRE
Set a key to expire after N seconds.
```
SET session "abc"
EXPIRE session 300     → (integer) 1  ← expiry set
EXPIRE nokey 300       → (integer) 0  ← key doesn't exist
```

### PEXPIRE
Same as EXPIRE but in milliseconds.
```
PEXPIRE session 30000    → (integer) 1
```

### TTL
Get remaining time-to-live in seconds.
```
TTL session    → (integer) 298   ← seconds left
TTL noexpiry   → (integer) -1    ← key exists but no expiry
TTL nokey      → (integer) -2    ← key doesn't exist
```

### PTTL
Same as TTL but returns milliseconds.
```
PTTL session    → (integer) 298000
```

### PERSIST
Remove the expiry from a key (make it permanent).
```
EXPIRE session 100
PERSIST session    → (integer) 1  ← expiry removed
TTL session        → (integer) -1 ← no longer expiring
```

---

## List Commands

Lists are ordered sequences of strings. Think of them as a double-ended queue.

### LPUSH / RPUSH
Push one or more values to the left (head) or right (tail) of a list.
```
LPUSH mylist "c" "b" "a"    → (integer) 3   ← new length
RPUSH mylist "d" "e"        → (integer) 5
```
Note: LPUSH pushes left, so "a" ends up at the front.

### LPOP / RPOP
Remove and return the leftmost or rightmost element.
```
LPOP mylist         → "a"
RPOP mylist         → "e"
LPOP mylist 2       → 1) "b"  2) "c"   ← pop multiple
```

### LLEN
Get the number of elements in a list.
```
LLEN mylist    → (integer) 3
LLEN nokey     → (integer) 0
```

### LRANGE
Get a range of elements by index (0-based). Negative indices count from the end.
```
RPUSH nums 1 2 3 4 5
LRANGE nums 0 -1    → 1) "1"  2) "2"  3) "3"  4) "4"  5) "5"
LRANGE nums 0 2     → 1) "1"  2) "2"  3) "3"
LRANGE nums -2 -1   → 1) "4"  2) "5"
```

### LINDEX
Get an element by its index.
```
LINDEX nums 0     → "1"
LINDEX nums -1    → "5"
LINDEX nums 99    → (nil)
```

### LSET
Set an element at a specific index.
```
LSET nums 0 "ten"
LINDEX nums 0    → "ten"
```

### LREM
Remove N occurrences of a value from the list.
```
RPUSH log "a" "b" "a" "c" "a"
LREM log 2 "a"    → (integer) 2  ← removed 2 occurrences
LREM log 0 "a"    → removes ALL occurrences
LREM log -1 "a"   → removes from tail
```

### LINSERT
Insert a value before or after a pivot element.
```
RPUSH steps "start" "end"
LINSERT steps BEFORE "end" "middle"
LRANGE steps 0 -1    → 1) "start"  2) "middle"  3) "end"
```

### LTRIM
Trim a list to only keep elements in the given range.
```
RPUSH q 1 2 3 4 5
LTRIM q 1 3
LRANGE q 0 -1    → 1) "2"  2) "3"  3) "4"
```

### BLPOP / BRPOP
Blocking pop — waits up to `timeout` seconds for an element to appear if the list is empty.
```
BLPOP mylist 5     # blocks up to 5 seconds, returns [key, value] or (nil)
BRPOP mylist 0     # 0 = block forever
```

---

## Hash Commands

Hashes are maps of field → value stored under a single key. Good for objects.

### HSET
Set one or more fields.
```
HSET user:1 name "Mohan" age "25" city "Hyderabad"
→ (integer) 3   ← number of NEW fields added
```

### HGET
Get a single field.
```
HGET user:1 name    → "Mohan"
HGET user:1 nofield → (nil)
```

### HMSET
Set multiple fields (legacy, prefer `HSET`).
```
HMSET user:1 name "Mohan" age "25"    → OK
```

### HMGET
Get multiple fields at once.
```
HMGET user:1 name age city
→ 1) "Mohan"  2) "25"  3) "Hyderabad"
```

### HDEL
Delete one or more fields.
```
HDEL user:1 city age    → (integer) 2
```

### HGETALL
Get all fields and values interleaved.
```
HGETALL user:1
→ 1) "name"  2) "Mohan"  3) "age"  4) "25"
```

### HKEYS / HVALS
Get just the field names or just the values.
```
HKEYS user:1    → 1) "name"  2) "age"
HVALS user:1    → 1) "Mohan"  2) "25"
```

### HEXISTS
Check if a field exists.
```
HEXISTS user:1 name     → (integer) 1
HEXISTS user:1 email    → (integer) 0
```

### HLEN
Count how many fields the hash has.
```
HLEN user:1    → (integer) 2
```

### HINCRBY
Increment a numeric hash field by an integer.
```
HSET stats views 100
HINCRBY stats views 50    → (integer) 150
```

### HINCRBYFLOAT
Increment a numeric hash field by a float.
```
HSET product price 9.99
HINCRBYFLOAT product price 0.01    → "10.0"
```

---

## Set Commands

Sets are unordered collections of unique strings. No duplicates.

### SADD
Add one or more members.
```
SADD fruits "apple" "banana" "mango"    → (integer) 3
SADD fruits "apple"                     → (integer) 0  ← already exists
```

### SREM
Remove one or more members.
```
SREM fruits "banana"    → (integer) 1
```

### SMEMBERS
Get all members (order is not guaranteed).
```
SMEMBERS fruits    → 1) "apple"  2) "mango"
```

### SISMEMBER
Check if a value is in the set.
```
SISMEMBER fruits "apple"    → (integer) 1
SISMEMBER fruits "grape"    → (integer) 0
```

### SCARD
Count the members.
```
SCARD fruits    → (integer) 2
```

### SUNION / SINTER / SDIFF
Set operations across multiple keys.
```
SADD a 1 2 3
SADD b 2 3 4

SUNION a b    → 1) "1"  2) "2"  3) "3"  4) "4"   ← all unique
SINTER a b    → 1) "2"  2) "3"                    ← common
SDIFF  a b    → 1) "1"                            ← in a but not b
```

### SUNIONSTORE / SINTERSTORE / SDIFFSTORE
Same as above but save the result into a new key.
```
SUNIONSTORE dest a b    → (integer) 4   ← size of result stored in "dest"
```

### SMOVE
Move a member from one set to another atomically.
```
SMOVE a b "1"    → (integer) 1  ← moved; 0 if member not in source
```

### SRANDMEMBER
Return a random member without removing it.
```
SRANDMEMBER fruits       → "apple"      ← one random member
SRANDMEMBER fruits 2     → 1) "mango"  2) "apple"   ← 2 random
```

### SPOP
Remove and return a random member.
```
SPOP fruits        → "apple"      ← removes and returns one
SPOP fruits 2      → removes and returns 2 random members
```

---

## Sorted Set Commands

Sorted sets are like sets but every member has a floating-point **score**. Members are always sorted by score.

### ZADD
Add members with scores.
```
ZADD leaderboard 100 "alice" 200 "bob" 150 "charlie"
→ (integer) 3
```

### ZREM
Remove members.
```
ZREM leaderboard "alice"    → (integer) 1
```

### ZSCORE
Get the score of a member.
```
ZSCORE leaderboard "bob"    → "200.0"
ZSCORE leaderboard "noone"  → (nil)
```

### ZRANK / ZREVRANK
Get the rank (0-based position) sorted ascending or descending.
```
ZRANK leaderboard "bob"       → (integer) 1   ← 0=lowest score
ZREVRANK leaderboard "bob"    → (integer) 0   ← 0=highest score
```

### ZRANGE / ZREVRANGE
Get members by rank range (ascending or descending).
```
ZRANGE leaderboard 0 -1              → all members, lowest to highest
ZRANGE leaderboard 0 -1 WITHSCORES  → includes scores
ZREVRANGE leaderboard 0 -1          → highest to lowest
```

### ZRANGEBYSCORE
Get members whose score falls within a range.
```
ZRANGEBYSCORE leaderboard 100 200              → members with score 100–200
ZRANGEBYSCORE leaderboard -inf +inf            → all members
ZRANGEBYSCORE leaderboard 100 200 WITHSCORES
```
Use `(100` for exclusive lower bound: `ZRANGEBYSCORE lb (100 200` means score > 100.

### ZCARD
Count the members.
```
ZCARD leaderboard    → (integer) 2
```

### ZINCRBY
Increase a member's score.
```
ZINCRBY leaderboard 50 "bob"    → "250.0"   ← new score
```

### ZCOUNT
Count members whose score is within a range.
```
ZCOUNT leaderboard 100 300    → (integer) 2
ZCOUNT leaderboard -inf +inf  → all
```

### ZREMRANGEBYRANK
Remove members by rank range.
```
ZREMRANGEBYRANK leaderboard 0 0    → removes the lowest-scored member
```

### ZREMRANGEBYSCORE
Remove members by score range.
```
ZREMRANGEBYSCORE leaderboard 0 100    → remove all with score ≤ 100
```

### ZRANGEBYLEX
Get members in lexicographic order (all members must have the same score).
```
ZADD dict 0 "apple" 0 "banana" 0 "cherry"
ZRANGEBYLEX dict "[a" "[b"    → members starting with a or b
ZRANGEBYLEX dict "-" "+"      → all members lexicographically
```
Bounds: `[` = inclusive, `(` = exclusive, `-` = negative infinity, `+` = positive infinity.

### ZLEXCOUNT
Count members in a lexicographic range.
```
ZLEXCOUNT dict "[a" "[c"    → (integer) 2
```

---

## Quick Reference — All Commands

| Category | Commands |
|----------|---------|
| General | `PING` |
| String | `SET` `GET` `DEL` `MSET` `MGET` `INCR` `DECR` `APPEND` `STRLEN` `SETNX` `GETSET` `GETDEL` `GETEX` |
| TTL/Expiry | `EXPIRE` `PEXPIRE` `TTL` `PTTL` `PERSIST` |
| List | `LPUSH` `RPUSH` `LPOP` `RPOP` `LLEN` `LRANGE` `LINDEX` `LSET` `LREM` `LINSERT` `LTRIM` `BLPOP` `BRPOP` |
| Hash | `HSET` `HMSET` `HGET` `HMGET` `HDEL` `HGETALL` `HKEYS` `HVALS` `HEXISTS` `HLEN` `HINCRBY` `HINCRBYFLOAT` |
| Set | `SADD` `SREM` `SMEMBERS` `SISMEMBER` `SCARD` `SUNION` `SINTER` `SDIFF` `SUNIONSTORE` `SINTERSTORE` `SDIFFSTORE` `SMOVE` `SRANDMEMBER` `SPOP` |
| Sorted Set | `ZADD` `ZREM` `ZSCORE` `ZRANK` `ZREVRANK` `ZRANGE` `ZREVRANGE` `ZRANGEBYSCORE` `ZCARD` `ZINCRBY` `ZCOUNT` `ZREMRANGEBYRANK` `ZREMRANGEBYSCORE` `ZRANGEBYLEX` `ZLEXCOUNT` |

**Total: 51 commands across 6 categories.**
