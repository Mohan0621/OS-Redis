package com.redis.db;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import com.redis.db.store.ExpirationManager;
import com.redis.db.store.HashStore;
import com.redis.db.store.ListStore;
import com.redis.db.store.SetStore;
import com.redis.db.store.SortedSetStore;
import com.redis.db.store.StringStore;
import com.redis.protocol.RespCommand;
import com.redis.server.AofLogger;

/**
 * Facade coordinating the five per-data-type stores, cross-type key expiration,
 * and AOF persistence.
 */
public class RedisDatabase {

    private final ExpirationManager expirations = new ExpirationManager();
    private final StringStore       strings     = new StringStore();
    private final ListStore         lists       = new ListStore(expirations);
    private final HashStore         hashes      = new HashStore(expirations);
    private final SetStore          sets        = new SetStore(expirations);
    private final SortedSetStore    zsets       = new SortedSetStore(expirations);

    private final AofLogger aofLogger;
    private boolean recovering = false;

    public RedisDatabase(AofLogger aofLogger) {
        this.aofLogger = aofLogger;
    }

    public StringStore strings()       { return strings; }
    public ListStore lists()           { return lists; }
    public HashStore hashes()          { return hashes; }
    public SetStore sets()             { return sets; }
    public SortedSetStore sortedSets() { return zsets; }

    public void setRecovering(boolean recovering) {
        this.recovering = recovering;
    }

    public void persist(RespCommand command) throws IOException {
        if (!recovering && aofLogger != null) {
            aofLogger.append(command.getRawBytes());
        }
    }

    // ================================================================
    // Cross-type expiration & key deletion
    // ================================================================

    private boolean purgeNonStringIfExpired(String key) {
        return expirations.purgeIfExpired(key, k -> {
            lists.removeRaw(k);
            hashes.removeRaw(k);
            sets.removeRaw(k);
            zsets.removeRaw(k);
        });
    }

    private boolean nonStringKeyExists(String key) {
        if (purgeNonStringIfExpired(key)) return false;
        return lists.containsKey(key)
                || hashes.containsKey(key)
                || sets.containsKey(key)
                || zsets.containsKey(key);
    }

    public void removeExpiredKeys() {
        strings.sweepExpired();
        expirations.sweepExpired(k -> {
            lists.removeRaw(k);
            hashes.removeRaw(k);
            sets.removeRaw(k);
            zsets.removeRaw(k);
        });
    }

    public String delete(String key) {
        String prev = strings.delete(key);
        expirations.clear(key);
        lists.removeRaw(key);
        hashes.removeRaw(key);
        sets.removeRaw(key);
        zsets.removeRaw(key);
        return prev;
    }

    public int del(String... keys) {
        int removed = 0;
        for (String key : keys) {
            boolean existed = strings.deleteIfActive(key);
            boolean nonStrExpired = expirations.isExpiredTimestamp(key);
            expirations.clear(key);
            boolean listRemoved = lists.removeRaw(key);
            boolean hashRemoved = hashes.removeRaw(key);
            boolean setRemoved = sets.removeRaw(key);
            boolean zsetRemoved = zsets.removeRaw(key);
            if (!nonStrExpired && (listRemoved || hashRemoved || setRemoved || zsetRemoved)) {
                existed = true;
            }
            if (existed) {
                removed++;
            }
        }
        return removed;
    }

    public int expire(String key, long seconds) {
        return pexpire(key, seconds * 1000L);
    }

    public int pexpire(String key, long millis) {
        int strResult = strings.pexpireIfPresent(key, millis);
        if (strResult != 0) {
            return strResult;
        }
        if (nonStringKeyExists(key)) {
            if (millis <= 0) {
                delete(key);
                return 1;
            }
            expirations.setExpirationMillis(key, System.currentTimeMillis() + millis);
            return 1;
        }
        return 0;
    }

    public int persist(String key) {
        if (strings.persistIfPresent(key) == 1) {
            return 1;
        }
        if (nonStringKeyExists(key)) {
            return expirations.clear(key) ? 1 : 0;
        }
        return 0;
    }

    public long ttl(String key) {
        long pttl = pttl(key);
        return pttl < 0 ? pttl : pttl / 1000L;
    }

    public long pttl(String key) {
        Long strPttl = strings.pttlIfPresent(key);
        if (strPttl != null) {
            return strPttl;
        }
        if (!nonStringKeyExists(key)) {
            return -2L;
        }
        Long exp = expirations.getExpirationMillis(key);
        if (exp == null) return -1L;
        return Math.max(0L, exp - System.currentTimeMillis());
    }

    // ================================================================
    // Delegated per-type operations (clean compatibility facade)
    // ================================================================

    // ---- String ----
    public void set(String key, String value, long expirationTimeMillis) { strings.set(key, value, expirationTimeMillis); }
    public String get(String key)                                        { return strings.get(key); }
    public String increment(String key)                                  { return strings.addToNumber(key, 1L); }
    public String decrement(String key)                                  { return strings.addToNumber(key, -1L); }
    public List<String> mget(String... keys)                             { return strings.mget(keys); }
    public void mset(String... keysAndValues)                            { strings.mset(keysAndValues); }
    public boolean setIfAbsent(String k, String v, long exp)             { return strings.setIfAbsent(k, v, exp); }
    public boolean setIfPresent(String k, String v, long exp)            { return strings.setIfPresent(k, v, exp); }
    public String getSet(String key, String value)                       { return strings.getSet(key, value); }
    public String getDel(String key)                                     { return strings.getDel(key); }
    public String getEx(String k, long exp, boolean p, boolean u)        { return strings.getEx(k, exp, p, u); }
    public int append(String key, String suffix)                         { return strings.append(key, suffix); }
    public int strlen(String key)                                        { return strings.strlen(key); }

    // ---- List ----
    public CompletableFuture<Void> registerListWaiter(String key)              { return lists.registerWaiter(key); }
    public void registerListWaiter(String key, CompletableFuture<Void> future) { lists.registerWaiter(key, future); }
    public void unregisterListWaiter(String key, CompletableFuture<Void> f)    { lists.unregisterWaiter(key, f); }
    public int lpush(String key, String... values)                             { return lists.lpush(key, values); }
    public int rpush(String key, String... values)                             { return lists.rpush(key, values); }
    public String lpop(String key)                                             { return lists.lpop(key); }
    public String rpop(String key)                                             { return lists.rpop(key); }
    public List<String> blpopTry(String... keys)                               { return lists.blpopTry(keys); }
    public List<String> brpopTry(String... keys)                               { return lists.brpopTry(keys); }
    public int llen(String key)                                                { return lists.llen(key); }
    public List<String> lrange(String key, int start, int stop)                { return lists.lrange(key, start, stop); }
    public String lindex(String key, int index)                                { return lists.lindex(key, index); }
    public void lset(String key, int index, String element)                    { lists.lset(key, index, element); }
    public int lrem(String key, int count, String element)                     { return lists.lrem(key, count, element); }
    public int linsert(String k, boolean before, String pivot, String el)      { return lists.linsert(k, before, pivot, el); }
    public void ltrim(String key, int start, int stop)                         { lists.ltrim(key, start, stop); }

    // ---- Hash ----
    public int hset(String key, String field, String value)                    { return hashes.hset(key, field, value); }
    public int hsetMultiple(String key, String[] args, int startIdx)           { return hashes.hsetMultiple(key, args, startIdx); }
    public String hget(String key, String field)                               { return hashes.hget(key, field); }
    public int hdel(String key, String... fields)                              { return hashes.hdel(key, fields); }
    public Map<String, String> hgetall(String key)                             { return hashes.hgetall(key); }
    public List<String> hmget(String key, String... fields)                    { return hashes.hmget(key, fields); }
    public List<String> hkeys(String key)                                      { return hashes.hkeys(key); }
    public List<String> hvals(String key)                                      { return hashes.hvals(key); }
    public boolean hexists(String key, String field)                           { return hashes.hexists(key, field); }
    public int hlen(String key)                                                { return hashes.hlen(key); }
    public long hincrby(String key, String field, long increment)              { return hashes.hincrby(key, field, increment); }
    public String hincrbyfloat(String key, String field, String incrementStr)  { return hashes.hincrbyfloat(key, field, incrementStr); }

    // ---- Set ----
    public int sadd(String key, String... members)                             { return sets.sadd(key, members); }
    public int srem(String key, String... members)                             { return sets.srem(key, members); }
    public boolean sismember(String key, String member)                        { return sets.sismember(key, member); }
    public int scard(String key)                                               { return sets.scard(key); }
    public Set<String> smembers(String key)                                    { return sets.smembers(key); }
    public Set<String> sunion(String... keys)                                  { return sets.sunion(keys); }
    public Set<String> sinter(String... keys)                                  { return sets.sinter(keys); }
    public Set<String> sdiff(String... keys)                                   { return sets.sdiff(keys); }
    public int sunionstore(String dest, String... keys)                        { return sets.sunionstore(dest, keys); }
    public int sinterstore(String dest, String... keys)                        { return sets.sinterstore(dest, keys); }
    public int sdiffstore(String dest, String... keys)                         { return sets.sdiffstore(dest, keys); }
    public int smove(String src, String dest, String member)                   { return sets.smove(src, dest, member); }
    public String srandmember(String key)                                      { return sets.srandmember(key); }
    public List<String> srandmember(String key, int count)                     { return sets.srandmember(key, count); }
    public String spop(String key)                                             { return sets.spop(key); }
    public List<String> spop(String key, int count)                            { return sets.spop(key, count); }

    // ---- Sorted Set ----
    public int zadd(String key, double score, String member)                   { return zsets.zadd(key, score, member); }
    public int zaddMultiple(String key, double[] scores, String[] members)     { return zsets.zaddMultiple(key, scores, members); }
    public int zrem(String key, String... members)                             { return zsets.zrem(key, members); }
    public List<String> zrange(String key, int start, int stop)                { return zsets.zrange(key, start, stop, false, false); }
    public List<String> zrange(String k, int s, int e, boolean ws, boolean rev){ return zsets.zrange(k, s, e, ws, rev); }
    public Double zscore(String key, String member)                            { return zsets.zscore(key, member); }
    public int zcard(String key)                                               { return zsets.zcard(key); }
    public Integer zrank(String key, String member)                            { return zsets.zrank(key, member); }
    public Integer zrevrank(String key, String member)                         { return zsets.zrevrank(key, member); }
    public double zincrby(String key, double delta, String member)             { return zsets.zincrby(key, delta, member); }
    public List<String> zrangeByScore(String k, String mn, String mx, boolean w){ return zsets.zrangeByScore(k, mn, mx, w); }
    public int zcount(String key, String minSpec, String maxSpec)              { return zsets.zcount(key, minSpec, maxSpec); }
    public int zremRangeByRank(String key, int start, int stop)                { return zsets.zremRangeByRank(key, start, stop); }
    public int zremRangeByScore(String key, String minSpec, String maxSpec)    { return zsets.zremRangeByScore(key, minSpec, maxSpec); }
    public List<String> zrangeByLex(String key, String minSpec, String maxSpec){ return zsets.zrangeByLex(key, minSpec, maxSpec); }
    public int zlexCount(String key, String minSpec, String maxSpec)           { return zsets.zlexCount(key, minSpec, maxSpec); }
}
