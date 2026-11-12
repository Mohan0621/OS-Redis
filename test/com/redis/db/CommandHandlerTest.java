package com.redis.db;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.redis.protocol.RespType;

class CommandHandlerTest {

    private RedisDatabase db;
    private CommandHandler handler;

    @BeforeEach
    void setUp() {
        db = new RedisDatabase(null);
        handler = new CommandHandler(db);
    }

    private CommandResult exec(String... args) {
        return handler.execute(args);
    }

    @Test
    void everyRoutedCommandInCommandRouterIsImplemented() {
        List<String> allRoutedVerbs = List.of(
                "PING",
                // String
                "GET", "SET", "DEL", "INCR", "DECR",
                "MGET", "MSET", "SETNX", "GETSET",
                "APPEND", "STRLEN", "TTL", "PTTL",
                "PERSIST", "EXPIRE", "PEXPIRE",
                "GETDEL", "GETEX",
                // List
                "LPUSH", "RPUSH", "LPOP", "RPOP",
                "LLEN", "LRANGE", "LINDEX", "LSET",
                "LREM", "LINSERT", "LTRIM",
                "BLPOP", "BRPOP",
                // Hash
                "HSET", "HGET", "HDEL", "HGETALL",
                "HMSET", "HMGET", "HKEYS", "HVALS",
                "HLEN", "HEXISTS", "HINCRBY", "HINCRBYFLOAT",
                // Set
                "SADD", "SREM", "SMEMBERS", "SISMEMBER",
                "SCARD", "SUNION", "SINTER", "SDIFF",
                "SUNIONSTORE", "SINTERSTORE", "SDIFFSTORE",
                "SMOVE", "SRANDMEMBER", "SPOP",
                // ZSet
                "ZADD", "ZREM", "ZSCORE", "ZRANK",
                "ZRANGE", "ZRANGEBYSCORE", "ZCARD",
                "ZINCRBY", "ZREVRANK", "ZREVRANGE",
                "ZRANGEBYLEX", "ZREMRANGEBYRANK",
                "ZREMRANGEBYSCORE", "ZCOUNT", "ZLEXCOUNT"
        );

        for (String verb : allRoutedVerbs) {
            CommandResult res = exec(verb);
            if (res.getType() == RespType.ERROR) {
                assertFalse(res.getValue().startsWith("ERR unknown command"),
                        "Routed command " + verb + " is still unimplemented: " + res.getValue());
            }
        }
    }

    @Test
    void stringCommandsAndExpiryLifecycle() throws InterruptedException {
        assertEquals("PONG", exec("PING").getValue());
        assertEquals("hi", exec("PING", "hi").getValue());

        assertEquals("OK", exec("SET", "k", "v").getValue());
        assertEquals("v", exec("GET", "k").getValue());
        assertEquals("2", exec("APPEND", "k", "2").getValue());
        assertEquals("2", exec("STRLEN", "k").getValue());
        assertEquals("v2", exec("GET", "k").getValue());

        // EXPIRE / PTTL / TTL / PERSIST
        assertEquals("-1", exec("TTL", "k").getValue());
        assertEquals("1", exec("EXPIRE", "k", "10").getValue());
        assertTrue(Long.parseLong(exec("TTL", "k").getValue()) >= 8);
        assertTrue(Long.parseLong(exec("PTTL", "k").getValue()) > 0);
        assertEquals("1", exec("PERSIST", "k").getValue());
        assertEquals("0", exec("PERSIST", "k").getValue());
        assertEquals("-1", exec("TTL", "k").getValue());

        // PEXPIRE short expiry
        assertEquals("1", exec("PEXPIRE", "k", "40").getValue());
        Thread.sleep(70);
        assertEquals(RespType.NULL, exec("GET", "k").getType());
        assertEquals("-2", exec("TTL", "k").getValue());
        assertEquals("-2", exec("PTTL", "k").getValue());

        // INCR / DECR preserve TTL
        exec("SET", "cnt", "10", "EX", "30");
        assertEquals("11", exec("INCR", "cnt").getValue());
        assertEquals("10", exec("DECR", "cnt").getValue());
        assertEquals("9", exec("DECR", "cnt").getValue());
        assertTrue(Long.parseLong(exec("TTL", "cnt").getValue()) > 0);

        // SETNX, GETSET, GETDEL, GETEX, MSET, MGET, DEL
        assertEquals("1", exec("SETNX", "nxkey", "first").getValue());
        assertEquals("0", exec("SETNX", "nxkey", "second").getValue());
        assertEquals("first", exec("GETSET", "nxkey", "updated").getValue());
        assertEquals("updated", exec("GETEX", "nxkey", "EX", "20").getValue());
        assertTrue(Long.parseLong(exec("TTL", "nxkey").getValue()) > 0);
        assertEquals("updated", exec("GETEX", "nxkey", "PERSIST").getValue());
        assertEquals("-1", exec("TTL", "nxkey").getValue());
        assertEquals("updated", exec("GETDEL", "nxkey").getValue());
        assertEquals(RespType.NULL, exec("GET", "nxkey").getType());

        assertEquals("OK", exec("MSET", "m1", "a", "m2", "b").getValue());
        assertEquals(Arrays.asList("a", null, "b"),
                exec("MGET", "m1", "missing", "m2").getElements());
        assertEquals("2", exec("DEL", "m1", "m2", "missing").getValue());
    }

    @Test
    void expireAndDeleteWorkOnNonStringKeys() throws InterruptedException {
        exec("LPUSH", "mylist", "x");
        assertEquals("1", exec("PEXPIRE", "mylist", "40").getValue());
        assertTrue(Long.parseLong(exec("PTTL", "mylist").getValue()) > 0);
        Thread.sleep(70);
        assertEquals("0", exec("LLEN", "mylist").getValue());
        assertEquals("-2", exec("TTL", "mylist").getValue());

        exec("SADD", "myset", "a");
        assertEquals("1", exec("DEL", "myset").getValue());
        assertEquals("0", exec("SCARD", "myset").getValue());
    }

    @Test
    void listCommandsWorkEndToEnd() {
        assertEquals("3", exec("RPUSH", "l", "a", "b", "c").getValue());
        assertEquals("4", exec("LPUSH", "l", "z").getValue());
        assertEquals(List.of("z", "a", "b", "c"), exec("LRANGE", "l", "0", "-1").getElements());

        assertEquals("b", exec("LINDEX", "l", "2").getValue());
        assertEquals("c", exec("LINDEX", "l", "-1").getValue());
        assertEquals("OK", exec("LSET", "l", "1", "A").getValue());
        assertEquals(RespType.ERROR, exec("LSET", "l", "99", "X").getType());

        assertEquals("5", exec("LINSERT", "l", "BEFORE", "b", "preB").getValue());
        assertEquals("6", exec("LINSERT", "l", "AFTER", "b", "postB").getValue());
        assertEquals("-1", exec("LINSERT", "l", "BEFORE", "missing", "x").getValue());
        assertEquals(List.of("z", "A", "preB", "b", "postB", "c"),
                exec("LRANGE", "l", "0", "-1").getElements());

        exec("RPUSH", "l", "dup", "dup", "dup");
        assertEquals("2", exec("LREM", "l", "2", "dup").getValue());
        assertEquals("OK", exec("LTRIM", "l", "1", "3").getValue());
        assertEquals(List.of("A", "preB", "b"), exec("LRANGE", "l", "0", "-1").getElements());

        // BLPOP / BRPOP non-blocking pop when data is already present
        assertEquals(List.of("l", "A"), exec("BLPOP", "empty", "l", "1").getElements());
        assertEquals(List.of("l", "b"), exec("BRPOP", "empty", "l", "1").getElements());
        assertEquals(RespType.NULL, exec("BLPOP", "empty", "0.1").getType());
    }

    @Test
    void hashCommandsWorkEndToEnd() {
        assertEquals("2", exec("HSET", "h", "f1", "10", "f2", "hello").getValue());
        assertEquals("OK", exec("HMSET", "h", "f3", "1.5", "f4", "world").getValue());
        assertEquals("4", exec("HLEN", "h").getValue());
        assertEquals("1", exec("HEXISTS", "h", "f1").getValue());

        assertEquals(Arrays.asList("10", null, "world"),
                exec("HMGET", "h", "f1", "missing", "f4").getElements());
        assertEquals(Set.of("f1", "f2", "f3", "f4"), Set.copyOf(exec("HKEYS", "h").getElements()));
        assertEquals(Set.of("10", "hello", "1.5", "world"),
                Set.copyOf(exec("HVALS", "h").getElements()));

        assertEquals("15", exec("HINCRBY", "h", "f1", "5").getValue());
        assertEquals("2", exec("HINCRBYFLOAT", "h", "f3", "0.5").getValue());
        assertEquals(RespType.ERROR, exec("HINCRBY", "h", "f2", "1").getType());

        assertEquals("2", exec("HDEL", "h", "f2", "f4", "missing").getValue());
        assertEquals("2", exec("HLEN", "h").getValue());
    }

    @Test
    void setCommandsWorkEndToEnd() {
        assertEquals("3", exec("SADD", "s1", "a", "b", "c").getValue());
        assertEquals("3", exec("SADD", "s2", "b", "c", "d").getValue());

        assertEquals(Set.of("a", "b", "c", "d"), Set.copyOf(exec("SUNION", "s1", "s2").getElements()));
        assertEquals(Set.of("b", "c"), Set.copyOf(exec("SINTER", "s1", "s2").getElements()));
        assertEquals(Set.of("a"), Set.copyOf(exec("SDIFF", "s1", "s2").getElements()));

        assertEquals("4", exec("SUNIONSTORE", "su", "s1", "s2").getValue());
        assertEquals("2", exec("SINTERSTORE", "si", "s1", "s2").getValue());
        assertEquals("1", exec("SDIFFSTORE", "sd", "s1", "s2").getValue());

        assertEquals("1", exec("SMOVE", "s1", "s2", "a").getValue());
        assertEquals("0", exec("SMOVE", "s1", "s2", "a").getValue());
        assertEquals("1", exec("SISMEMBER", "s2", "a").getValue());

        assertNotNull(exec("SRANDMEMBER", "s2").getValue());
        assertEquals(2, exec("SRANDMEMBER", "s2", "2").getElements().size());
        assertEquals(5, exec("SRANDMEMBER", "s2", "-5").getElements().size());

        String popped = exec("SPOP", "s2").getValue();
        assertNotNull(popped);
        assertEquals("0", exec("SISMEMBER", "s2", popped).getValue());
        assertEquals(2, exec("SPOP", "s2", "2").getElements().size());
    }
}
