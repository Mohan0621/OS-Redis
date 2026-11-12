package com.redis.db;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.redis.protocol.RespType;

class SortedSetTest {

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
    void supportsDuplicateScoresOrderedLexicographically() {
        assertEquals("1", exec("ZADD", "zs", "10", "charlie").getValue());
        assertEquals("1", exec("ZADD", "zs", "10", "alpha").getValue());
        assertEquals("1", exec("ZADD", "zs", "10", "bravo").getValue());
        assertEquals("1", exec("ZADD", "zs", "5", "zero").getValue());

        assertEquals("4", exec("ZCARD", "zs").getValue());
        assertEquals(List.of("zero", "alpha", "bravo", "charlie"),
                exec("ZRANGE", "zs", "0", "-1").getElements());
        assertEquals(List.of("charlie", "bravo", "alpha", "zero"),
                exec("ZREVRANGE", "zs", "0", "-1").getElements());

        assertEquals("1", exec("ZRANK", "zs", "alpha").getValue());
        assertEquals("2", exec("ZRANK", "zs", "bravo").getValue());
        assertEquals("1", exec("ZREVRANK", "zs", "bravo").getValue());
    }

    @Test
    void updatingMemberScoreMovesItWithoutCreatingDuplicates() {
        exec("ZADD", "zs", "10", "a", "10", "b", "20", "c");

        // Update "b" from 10 to 30: returns 0 (updated, not added)
        assertEquals("0", exec("ZADD", "zs", "30", "b").getValue());
        assertEquals("3", exec("ZCARD", "zs").getValue());
        assertEquals(30.0, Double.parseDouble(exec("ZSCORE", "zs", "b").getValue()));
        assertEquals(List.of("a", "c", "b"), exec("ZRANGE", "zs", "0", "-1").getElements());
    }

    @Test
    void zremRemovesOnlyTargetMemberSharingScore() {
        exec("ZADD", "zs", "1.5", "m1", "1.5", "m2", "1.5", "m3");
        assertEquals("1", exec("ZREM", "zs", "m2").getValue());
        assertEquals("2", exec("ZCARD", "zs").getValue());
        assertEquals(List.of("m1", "m3"), exec("ZRANGE", "zs", "0", "-1").getElements());
        assertEquals(RespType.NULL, exec("ZSCORE", "zs", "m2").getType());
    }

    @Test
    void zincrbyAndWithScoresWorkCorrectly() {
        assertEquals("2.5", exec("ZINCRBY", "zs", "2.5", "player1").getValue());
        assertEquals("7.5", exec("ZINCRBY", "zs", "5.0", "player1").getValue());
        exec("ZADD", "zs", "7.5", "player0");

        List<String> withScores = exec("ZRANGE", "zs", "0", "-1", "WITHSCORES").getElements();
        assertEquals(List.of("player0", "7.5", "player1", "7.5"), withScores);
    }

    @Test
    void rangeByScoreCountAndRemRangeWorkWithInclusiveAndExclusiveBounds() {
        exec("ZADD", "zs", "1", "a", "2", "b", "2", "b2", "3", "c", "4", "d");

        assertEquals("4", exec("ZCOUNT", "zs", "1", "3").getValue());
        assertEquals("2", exec("ZCOUNT", "zs", "(1", "(3").getValue());
        assertEquals(List.of("b", "b2"), exec("ZRANGEBYSCORE", "zs", "(1", "(3").getElements());
        assertEquals(List.of("a", "1.0", "b", "2.0", "b2", "2.0"),
                exec("ZRANGEBYSCORE", "zs", "-inf", "2", "WITHSCORES").getElements());

        // Inverted range returns empty/0 instead of throwing
        assertEquals(List.of(), exec("ZRANGEBYSCORE", "zs", "10", "1").getElements());
        assertEquals("0", exec("ZCOUNT", "zs", "(2", "(2").getValue());

        assertEquals("2", exec("ZREMRANGEBYSCORE", "zs", "2", "(3").getValue());
        assertEquals(List.of("a", "c", "d"), exec("ZRANGE", "zs", "0", "-1").getElements());

        assertEquals("1", exec("ZREMRANGEBYRANK", "zs", "-1", "-1").getValue());
        assertEquals(List.of("a", "c"), exec("ZRANGE", "zs", "0", "-1").getElements());
    }

    @Test
    void rangeByLexAndLexCountWorkCorrectly() {
        exec("ZADD", "zs", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e");

        assertEquals(List.of("b", "c", "d"), exec("ZRANGEBYLEX", "zs", "[b", "[d").getElements());
        assertEquals(List.of("b", "c"), exec("ZRANGEBYLEX", "zs", "(a", "(d").getElements());
        assertEquals(List.of("a", "b", "c", "d", "e"), exec("ZRANGEBYLEX", "zs", "-", "+").getElements());
        assertEquals("3", exec("ZLEXCOUNT", "zs", "[b", "(e").getValue());
        assertEquals(RespType.ERROR, exec("ZRANGEBYLEX", "zs", "bad", "+").getType());
    }
}

