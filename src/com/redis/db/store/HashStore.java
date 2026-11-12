package com.redis.db.store;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Store for Redis Hash keys (field -> value maps).
 */
public class HashStore {

    private final Map<String, Map<String, String>> hashes = new ConcurrentHashMap<>();
    private final ExpirationManager expirations;

    public HashStore(ExpirationManager expirations) {
        this.expirations = expirations;
    }

    public boolean containsKey(String key) {
        return hashes.containsKey(key);
    }

    public boolean removeRaw(String key) {
        return hashes.remove(key) != null;
    }

    private boolean isExpired(String key) {
        return expirations.purgeIfExpired(key, hashes::remove);
    }

    public int hset(String key, String field, String value) {
        isExpired(key);
        int[] added = new int[1];
        hashes.compute(key, (k, existing) -> {
            Map<String, String> hash = existing != null ? existing : new HashMap<>();
            boolean isNew = !hash.containsKey(field);
            hash.put(field, value);
            added[0] = isNew ? 1 : 0;
            return hash;
        });
        return added[0];
    }

    public int hsetMultiple(String key, String[] fieldValueArgs, int startIdx) {
        isExpired(key);
        int[] added = new int[1];
        hashes.compute(key, (k, existing) -> {
            Map<String, String> hash = existing != null ? existing : new HashMap<>();
            for (int i = startIdx; i < fieldValueArgs.length; i += 2) {
                String f = fieldValueArgs[i];
                String v = fieldValueArgs[i + 1];
                if (!hash.containsKey(f)) {
                    added[0]++;
                }
                hash.put(f, v);
            }
            return hash;
        });
        return added[0];
    }

    public String hget(String key, String field) {
        if (isExpired(key)) return null;
        Map<String, String> hash = hashes.get(key);
        if (hash == null) return null;
        synchronized (hash) {
            return hash.get(field);
        }
    }

    public int hdel(String key, String... fields) {
        if (isExpired(key)) return 0;
        int[] removed = new int[1];
        hashes.computeIfPresent(key, (k, hash) -> {
            for (String f : fields) {
                if (hash.remove(f) != null) {
                    removed[0]++;
                }
            }
            if (hash.isEmpty()) {
                expirations.clear(k);
                return null;
            }
            return hash;
        });
        return removed[0];
    }

    public Map<String, String> hgetall(String key) {
        if (isExpired(key)) return Collections.emptyMap();
        Map<String, String> hash = hashes.get(key);
        if (hash == null) return Collections.emptyMap();
        synchronized (hash) {
            return Collections.unmodifiableMap(new HashMap<>(hash));
        }
    }

    public List<String> hmget(String key, String... fields) {
        if (isExpired(key)) {
            return Collections.nCopies(fields.length, null);
        }
        Map<String, String> hash = hashes.get(key);
        List<String> result = new ArrayList<>(fields.length);
        if (hash == null) {
            for (int i = 0; i < fields.length; i++) result.add(null);
            return result;
        }
        synchronized (hash) {
            for (String f : fields) {
                result.add(hash.get(f));
            }
        }
        return result;
    }

    public List<String> hkeys(String key) {
        if (isExpired(key)) return Collections.emptyList();
        Map<String, String> hash = hashes.get(key);
        if (hash == null) return Collections.emptyList();
        synchronized (hash) {
            return new ArrayList<>(hash.keySet());
        }
    }

    public List<String> hvals(String key) {
        if (isExpired(key)) return Collections.emptyList();
        Map<String, String> hash = hashes.get(key);
        if (hash == null) return Collections.emptyList();
        synchronized (hash) {
            return new ArrayList<>(hash.values());
        }
    }

    public boolean hexists(String key, String field) {
        if (isExpired(key)) return false;
        Map<String, String> hash = hashes.get(key);
        if (hash == null) return false;
        synchronized (hash) {
            return hash.containsKey(field);
        }
    }

    public int hlen(String key) {
        if (isExpired(key)) return 0;
        Map<String, String> hash = hashes.get(key);
        if (hash == null) return 0;
        synchronized (hash) {
            return hash.size();
        }
    }

    public long hincrby(String key, String field, long increment) {
        isExpired(key);
        long[] updated = new long[1];
        hashes.compute(key, (k, existing) -> {
            Map<String, String> hash = existing != null ? existing : new HashMap<>();
            String current = hash.get(field);
            long val = current == null ? 0L : Long.parseLong(current);
            val = Math.addExact(val, increment);
            hash.put(field, String.valueOf(val));
            updated[0] = val;
            return hash;
        });
        return updated[0];
    }

    public String hincrbyfloat(String key, String field, String incrementStr) {
        double parsedInc = Double.parseDouble(incrementStr);
        if (Double.isNaN(parsedInc) || Double.isInfinite(parsedInc)) {
            throw new IllegalArgumentException("value is not a valid float");
        }
        BigDecimal delta = new BigDecimal(incrementStr);

        isExpired(key);
        String[] updated = new String[1];
        hashes.compute(key, (k, existing) -> {
            Map<String, String> hash = existing != null ? existing : new HashMap<>();
            String current = hash.get(field);
            BigDecimal base;
            if (current == null) {
                base = BigDecimal.ZERO;
            } else {
                double d = Double.parseDouble(current);
                if (Double.isNaN(d) || Double.isInfinite(d)) {
                    throw new NumberFormatException("hash value is not a float");
                }
                base = new BigDecimal(current);
            }
            BigDecimal sum = base.add(delta).stripTrailingZeros();
            String formatted = sum.compareTo(BigDecimal.ZERO) == 0 ? "0" : sum.toPlainString();
            hash.put(field, formatted);
            updated[0] = formatted;
            return hash;
        });
        return updated[0];
    }
}

