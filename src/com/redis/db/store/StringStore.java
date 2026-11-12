package com.redis.db.store;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.redis.db.RedisEntry;

/**
 * Thread-safe store for Redis String keys and their per-entry TTLs.
 */
public class StringStore {

    private final Map<String, RedisEntry> data = new ConcurrentHashMap<>();

    public void set(String key, String value, long expirationTimeMillis) {
        data.put(key, new RedisEntry(value, expirationTimeMillis));
    }

    public String get(String key) {
        RedisEntry entry = data.get(key);
        if (entry == null) return null;
        if (entry.isExpired()) {
            data.remove(key, entry);
            return null;
        }
        return entry.getValue();
    }

    public String delete(String key) {
        RedisEntry entry = data.remove(key);
        return entry != null ? entry.getValue() : null;
    }

    public boolean deleteIfActive(String key) {
        RedisEntry entry = data.remove(key);
        return entry != null && !entry.isExpired();
    }

    public String addToNumber(String key, long delta) {
        RedisEntry[] result = new RedisEntry[1];
        data.compute(key, (k, oldEntry) -> {
            long number = 0L;
            long exp = -1L;
            if (oldEntry != null && !oldEntry.isExpired()) {
                number = Long.parseLong(oldEntry.getValue());
                exp = oldEntry.getExpirationTimeMillis();
            }
            number = Math.addExact(number, delta);
            result[0] = new RedisEntry(String.valueOf(number), exp);
            return result[0];
        });
        return result[0].getValue();
    }

    public List<String> mget(String... keys) {
        List<String> values = new ArrayList<>(keys.length);
        for (String key : keys) {
            values.add(get(key));
        }
        return values;
    }

    public void mset(String... keysAndValues) {
        for (int i = 0; i < keysAndValues.length; i += 2) {
            set(keysAndValues[i], keysAndValues[i + 1], -1L);
        }
    }

    public boolean setIfAbsent(String key, String value, long expirationTimeMillis) {
        removeIfExpired(key);
        return data.putIfAbsent(key, new RedisEntry(value, expirationTimeMillis)) == null;
    }

    public boolean setIfPresent(String key, String value, long expirationTimeMillis) {
        removeIfExpired(key);
        return data.replace(key, new RedisEntry(value, expirationTimeMillis)) != null;
    }

    public String getSet(String key, String value) {
        String[] previous = new String[1];
        data.compute(key, (k, oldEntry) -> {
            if (oldEntry != null && !oldEntry.isExpired()) {
                previous[0] = oldEntry.getValue();
            }
            return new RedisEntry(value, -1L);
        });
        return previous[0];
    }

    public String getDel(String key) {
        RedisEntry entry = data.remove(key);
        if (entry == null || entry.isExpired()) {
            return null;
        }
        return entry.getValue();
    }

    public String getEx(String key, long expirationTimeMillis, boolean persist, boolean updateExpiry) {
        String[] result = new String[1];
        data.computeIfPresent(key, (k, oldEntry) -> {
            if (oldEntry.isExpired()) {
                return null;
            }
            result[0] = oldEntry.getValue();
            if (persist) {
                return new RedisEntry(oldEntry.getValue(), -1L);
            }
            if (updateExpiry) {
                if (expirationTimeMillis <= System.currentTimeMillis()) {
                    return null;
                }
                return new RedisEntry(oldEntry.getValue(), expirationTimeMillis);
            }
            return oldEntry;
        });
        return result[0];
    }

    public int append(String key, String suffix) {
        int[] newLen = new int[1];
        data.compute(key, (k, oldEntry) -> {
            String nextVal;
            long exp = -1L;
            if (oldEntry != null && !oldEntry.isExpired()) {
                nextVal = oldEntry.getValue() + suffix;
                exp = oldEntry.getExpirationTimeMillis();
            } else {
                nextVal = suffix;
            }
            newLen[0] = nextVal.length();
            return new RedisEntry(nextVal, exp);
        });
        return newLen[0];
    }

    public int strlen(String key) {
        String val = get(key);
        return val == null ? 0 : val.length();
    }

    /**
     * Updates expiration for an existing string key.
     * Returns 1 if the key existed and was updated/deleted, 0 if absent/expired.
     */
    public int pexpireIfPresent(String key, long millis) {
        RedisEntry entry = data.get(key);
        if (entry == null) return 0;
        if (entry.isExpired()) {
            data.remove(key, entry);
            return 0;
        }
        if (millis <= 0) {
            data.remove(key, entry);
            return 1;
        }
        long newExp = System.currentTimeMillis() + millis;
        data.computeIfPresent(key, (k, e) ->
                e.isExpired() ? null : new RedisEntry(e.getValue(), newExp));
        return data.containsKey(key) ? 1 : 0;
    }

    public int persistIfPresent(String key) {
        int[] updated = new int[1];
        data.computeIfPresent(key, (k, e) -> {
            if (e.isExpired()) {
                return null;
            }
            if (e.getExpirationTimeMillis() == -1L) {
                return e;
            }
            updated[0] = 1;
            return new RedisEntry(e.getValue(), -1L);
        });
        return updated[0];
    }

    /**
     * Returns remaining millis if key exists in StringStore, -1 if persistent,
     * or null if not present / expired in StringStore.
     */
    public Long pttlIfPresent(String key) {
        RedisEntry entry = data.get(key);
        if (entry == null) return null;
        if (entry.isExpired()) {
            data.remove(key, entry);
            return null;
        }
        if (entry.getExpirationTimeMillis() == -1L) return -1L;
        return Math.max(0L, entry.getExpirationTimeMillis() - System.currentTimeMillis());
    }

    public void sweepExpired() {
        for (Map.Entry<String, RedisEntry> entry : data.entrySet()) {
            RedisEntry redisEntry = entry.getValue();
            if (redisEntry.isExpired()) {
                data.remove(entry.getKey(), redisEntry);
            }
        }
    }

    private void removeIfExpired(String key) {
        RedisEntry entry = data.get(key);
        if (entry != null && entry.isExpired()) {
            data.remove(key, entry);
        }
    }
}

