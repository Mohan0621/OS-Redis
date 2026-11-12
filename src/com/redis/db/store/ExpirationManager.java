package com.redis.db.store;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Tracks epoch-millisecond expiration timestamps for non-string keys
 * (lists, hashes, sets, and sorted sets).
 */
public class ExpirationManager {

    private final Map<String, Long> keyExpirations = new ConcurrentHashMap<>();

    public void setExpirationMillis(String key, long expirationTimeMillis) {
        keyExpirations.put(key, expirationTimeMillis);
    }

    public Long getExpirationMillis(String key) {
        return keyExpirations.get(key);
    }

    public boolean clear(String key) {
        return keyExpirations.remove(key) != null;
    }

    public boolean isExpiredTimestamp(String key) {
        Long exp = keyExpirations.get(key);
        return exp != null && System.currentTimeMillis() >= exp;
    }

    public boolean purgeIfExpired(String key, Consumer<String> onExpired) {
        Long exp = keyExpirations.get(key);
        if (exp != null && System.currentTimeMillis() >= exp) {
            if (keyExpirations.remove(key, exp)) {
                onExpired.accept(key);
            }
            return true;
        }
        return false;
    }

    public void sweepExpired(Consumer<String> onExpired) {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, Long> entry : keyExpirations.entrySet()) {
            Long exp = entry.getValue();
            if (exp != null && now >= exp) {
                String key = entry.getKey();
                if (keyExpirations.remove(key, exp)) {
                    onExpired.accept(key);
                }
            }
        }
    }
}

