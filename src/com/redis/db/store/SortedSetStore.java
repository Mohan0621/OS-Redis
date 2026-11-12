package com.redis.db.store;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.redis.db.store.SortedSet.LexBound;
import com.redis.db.store.SortedSet.ScoreBound;

/**
 * Store for Redis Sorted Set keys backed by {@link SortedSet}.
 */
public class SortedSetStore {

    private final Map<String, SortedSet> zsets = new ConcurrentHashMap<>();
    private final ExpirationManager expirations;

    public SortedSetStore(ExpirationManager expirations) {
        this.expirations = expirations;
    }

    public boolean containsKey(String key) {
        return zsets.containsKey(key);
    }

    public boolean removeRaw(String key) {
        return zsets.remove(key) != null;
    }

    private boolean isExpired(String key) {
        return expirations.purgeIfExpired(key, zsets::remove);
    }

    public int zadd(String key, double score, String member) {
        isExpired(key);
        int[] added = new int[1];
        zsets.compute(key, (k, existing) -> {
            SortedSet zset = existing != null ? existing : new SortedSet();
            added[0] = zset.add(score, member);
            return zset;
        });
        return added[0];
    }

    public int zaddMultiple(String key, double[] scores, String[] members) {
        isExpired(key);
        int[] added = new int[1];
        zsets.compute(key, (k, existing) -> {
            SortedSet zset = existing != null ? existing : new SortedSet();
            for (int i = 0; i < members.length; i++) {
                added[0] += zset.add(scores[i], members[i]);
            }
            return zset;
        });
        return added[0];
    }

    public int zrem(String key, String... members) {
        if (isExpired(key)) return 0;
        int[] removed = new int[1];
        zsets.computeIfPresent(key, (k, zset) -> {
            for (String m : members) {
                removed[0] += zset.remove(m);
            }
            if (zset.isEmpty()) {
                expirations.clear(k);
                return null;
            }
            return zset;
        });
        return removed[0];
    }

    public List<String> zrange(String key, int start, int stop, boolean withScores, boolean reverse) {
        if (isExpired(key)) return Collections.emptyList();
        SortedSet zset = zsets.get(key);
        return zset == null ? Collections.emptyList() : zset.range(start, stop, withScores, reverse);
    }

    public Double zscore(String key, String member) {
        if (isExpired(key)) return null;
        SortedSet zset = zsets.get(key);
        return zset == null ? null : zset.score(member);
    }

    public int zcard(String key) {
        if (isExpired(key)) return 0;
        SortedSet zset = zsets.get(key);
        return zset == null ? 0 : zset.size();
    }

    public Integer zrank(String key, String member) {
        if (isExpired(key)) return null;
        SortedSet zset = zsets.get(key);
        return zset == null ? null : zset.rank(member);
    }

    public Integer zrevrank(String key, String member) {
        if (isExpired(key)) return null;
        SortedSet zset = zsets.get(key);
        return zset == null ? null : zset.revRank(member);
    }

    public double zincrby(String key, double delta, String member) {
        if (Double.isNaN(delta)) {
            throw new IllegalArgumentException("resulting score is not a number (NaN)");
        }
        isExpired(key);
        double[] updated = new double[1];
        zsets.compute(key, (k, existing) -> {
            SortedSet zset = existing != null ? existing : new SortedSet();
            updated[0] = zset.incrBy(delta, member);
            return zset;
        });
        return updated[0];
    }

    public List<String> zrangeByScore(String key, String minSpec, String maxSpec, boolean withScores) {
        ScoreBound min = SortedSet.parseScoreBound(minSpec);
        ScoreBound max = SortedSet.parseScoreBound(maxSpec);
        if (isExpired(key)) return Collections.emptyList();
        SortedSet zset = zsets.get(key);
        return zset == null
                ? Collections.emptyList()
                : zset.rangeByScore(min.value(), min.inclusive(), max.value(), max.inclusive(), withScores);
    }

    public int zcount(String key, String minSpec, String maxSpec) {
        ScoreBound min = SortedSet.parseScoreBound(minSpec);
        ScoreBound max = SortedSet.parseScoreBound(maxSpec);
        if (isExpired(key)) return 0;
        SortedSet zset = zsets.get(key);
        return zset == null
                ? 0
                : zset.count(min.value(), min.inclusive(), max.value(), max.inclusive());
    }

    public int zremRangeByRank(String key, int start, int stop) {
        if (isExpired(key)) return 0;
        int[] removed = new int[1];
        zsets.computeIfPresent(key, (k, zset) -> {
            removed[0] = zset.remRangeByRank(start, stop);
            if (zset.isEmpty()) {
                expirations.clear(k);
                return null;
            }
            return zset;
        });
        return removed[0];
    }

    public int zremRangeByScore(String key, String minSpec, String maxSpec) {
        ScoreBound min = SortedSet.parseScoreBound(minSpec);
        ScoreBound max = SortedSet.parseScoreBound(maxSpec);
        if (isExpired(key)) return 0;
        int[] removed = new int[1];
        zsets.computeIfPresent(key, (k, zset) -> {
            removed[0] = zset.remRangeByScore(min.value(), min.inclusive(), max.value(), max.inclusive());
            if (zset.isEmpty()) {
                expirations.clear(k);
                return null;
            }
            return zset;
        });
        return removed[0];
    }

    public List<String> zrangeByLex(String key, String minSpec, String maxSpec) {
        LexBound min = SortedSet.parseLexBound(minSpec);
        LexBound max = SortedSet.parseLexBound(maxSpec);
        if (isExpired(key)) return Collections.emptyList();
        SortedSet zset = zsets.get(key);
        return zset == null ? Collections.emptyList() : zset.rangeByLex(min, max);
    }

    public int zlexCount(String key, String minSpec, String maxSpec) {
        LexBound min = SortedSet.parseLexBound(minSpec);
        LexBound max = SortedSet.parseLexBound(maxSpec);
        if (isExpired(key)) return 0;
        SortedSet zset = zsets.get(key);
        return zset == null ? 0 : zset.lexCount(min, max);
    }
}

