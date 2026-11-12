package com.redis.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import com.redis.protocol.RespCommand;
import com.redis.server.AofLogger;

public class RedisDatabase {

    // ---- String store ----
    private final Map<String, RedisEntry> data = new ConcurrentHashMap<>();

    // ---- List store  (key → doubly-linked list of strings) ----
    // Each list is only ever accessed from the single partition worker that
    // owns that key, so plain LinkedList is safe — no internal synchronisation.
    private final Map<String, LinkedList<String>> lists = new ConcurrentHashMap<>();

    // ---- Hash store  (key → field→value map) ----
    private final Map<String, Map<String, String>> hashes = new ConcurrentHashMap<>();

    // ---- Set store   (key → insertion-ordered set) ----
    private final Map<String, LinkedHashSet<String>> sets = new ConcurrentHashMap<>();

    // ---- Sorted-set store  (key → score→member map, sorted by score) ----
    // TreeMap keeps entries sorted by score automatically.
    private final Map<String, TreeMap<Double, String>> zsets = new ConcurrentHashMap<>();

    private final AofLogger aofLogger;
    private boolean recovering = false;

    public RedisDatabase(AofLogger aofLogger) {
        this.aofLogger = aofLogger;
    }

    public void setRecovering(boolean recovering) {
        this.recovering = recovering;
    }

    public void persist(RespCommand command) throws IOException {
        if (!recovering) {
            aofLogger.append(command.getRawBytes());
        }
    }

    public void set(String key, String value, long expirationTimeMillis) {
        data.put(key, new RedisEntry(value, expirationTimeMillis));
        // AOF logging is handled via persist(RespCommand) using raw RESP bytes from the client
    }

    public String get(String key) {
        RedisEntry entry = data.get(key);
        if (entry == null) return null;
        if (entry.isExpired()) {
            data.remove(key);
            return null;
        }
        return entry.getValue();
    }

    public String delete(String key) {
        RedisEntry entry = data.remove(key);
        return entry != null ? entry.getValue() : null;
    }

    public String increment(String key) {
        RedisEntry[] result = new RedisEntry[1];

        data.compute(key, (k, oldEntry) -> {
            int number = 0;
            if (oldEntry != null && !oldEntry.isExpired()) {
                number = Integer.parseInt(oldEntry.getValue());
            }
            number++;
            result[0] = new RedisEntry(String.valueOf(number), -1);
            return result[0];
        });

        return result[0].getValue();
    }

    public void removeExpiredKeys() {
        for (Map.Entry<String, RedisEntry> entry : data.entrySet()) {
            RedisEntry redisEntry = entry.getValue();
            if (redisEntry.isExpired()) {
                data.remove(entry.getKey(), redisEntry);
            }
        }
    }

    public long ttl(String key) {
        RedisEntry entry = data.get(key);
        if (entry == null) return -2;
        if (entry.isExpired()) {
            data.remove(key, entry);
            return -2;
        }
        if (entry.getExpirationTimeMillis() == -1) return -1;
        long remainingMillis = entry.getExpirationTimeMillis() - System.currentTimeMillis();
        return Math.max(0, remainingMillis / 1000);
    }

    public boolean setIfAbsent(String key, String value, long expirationTimeMillis) {
        removeIfExpired(key);
        return data.putIfAbsent(key, new RedisEntry(value, expirationTimeMillis)) == null;
    }

    public boolean setIfPresent(String key, String value, long expirationTimeMillis) {
        return data.replace(key, new RedisEntry(value, expirationTimeMillis)) != null;
    }

    private void removeIfExpired(String key) {
        RedisEntry entry = data.get(key);
        if (entry != null && entry.isExpired()) {
            data.remove(key, entry);
        }
    }

    // ================================================================
    // List operations
    // ================================================================

    /**
     * Push one value to the head (left) of a list.
     * Creates the list if it does not exist.
     * Returns the new length.
     */
    public int lpush(String key, String value) {
        LinkedList<String> list = lists.computeIfAbsent(key, k -> new LinkedList<>());
        list.addFirst(value);
        return list.size();
    }

    /**
     * Push one value to the tail (right) of a list.
     * Creates the list if it does not exist.
     * Returns the new length.
     */
    public int rpush(String key, String value) {
        LinkedList<String> list = lists.computeIfAbsent(key, k -> new LinkedList<>());
        list.addLast(value);
        return list.size();
    }

    /**
     * Remove and return the head element, or null if the list is empty/absent.
     */
    public String lpop(String key) {
        LinkedList<String> list = lists.get(key);
        if (list == null || list.isEmpty()) return null;
        String value = list.removeFirst();
        if (list.isEmpty()) lists.remove(key);
        return value;
    }

    /**
     * Remove and return the tail element, or null if the list is empty/absent.
     */
    public String rpop(String key) {
        LinkedList<String> list = lists.get(key);
        if (list == null || list.isEmpty()) return null;
        String value = list.removeLast();
        if (list.isEmpty()) lists.remove(key);
        return value;
    }

    /**
     * Return the number of elements in the list, or 0 if absent.
     */
    public int llen(String key) {
        LinkedList<String> list = lists.get(key);
        return list == null ? 0 : list.size();
    }

    /**
     * Return elements from index start to stop (inclusive, 0-based).
     * Negative indices count from the tail: -1 is the last element.
     */
    public List<String> lrange(String key, int start, int stop) {
        LinkedList<String> list = lists.get(key);
        if (list == null || list.isEmpty()) return Collections.emptyList();

        int size = list.size();
        // Normalise negative indices
        if (start < 0) start = Math.max(0, size + start);
        if (stop  < 0) stop  = size + stop;
        stop = Math.min(stop, size - 1);

        if (start > stop) return Collections.emptyList();

        List<String> result = new ArrayList<>();
        int i = 0;
        for (String v : list) {
            if (i >= start && i <= stop) result.add(v);
            if (i > stop) break;
            i++;
        }
        return result;
    }

    // ================================================================
    // Hash operations
    // ================================================================

    /**
     * Set field to value in the hash stored at key.
     * Returns 1 if a new field was added, 0 if an existing field was updated.
     */
    public int hset(String key, String field, String value) {
        Map<String, String> hash = hashes.computeIfAbsent(key, k -> new HashMap<>());
        boolean isNew = !hash.containsKey(field);
        hash.put(field, value);
        return isNew ? 1 : 0;
    }

    /**
     * Get the value of a field, or null if the key or field does not exist.
     */
    public String hget(String key, String field) {
        Map<String, String> hash = hashes.get(key);
        return hash == null ? null : hash.get(field);
    }

    /**
     * Delete a field. Returns 1 if the field existed, 0 otherwise.
     */
    public int hdel(String key, String field) {
        Map<String, String> hash = hashes.get(key);
        if (hash == null) return 0;
        boolean removed = hash.remove(field) != null;
        if (hash.isEmpty()) hashes.remove(key);
        return removed ? 1 : 0;
    }

    /**
     * Returns all field→value pairs, or an empty map if the key is absent.
     */
    public Map<String, String> hgetall(String key) {
        Map<String, String> hash = hashes.get(key);
        return hash == null ? Collections.emptyMap() : Collections.unmodifiableMap(hash);
    }

    /**
     * Returns true if the field exists in the hash.
     */
    public boolean hexists(String key, String field) {
        Map<String, String> hash = hashes.get(key);
        return hash != null && hash.containsKey(field);
    }

    /**
     * Returns the number of fields in the hash, or 0 if absent.
     */
    public int hlen(String key) {
        Map<String, String> hash = hashes.get(key);
        return hash == null ? 0 : hash.size();
    }

    // ================================================================
    // Set operations
    // ================================================================

    /**
     * Add a member to the set. Returns 1 if added, 0 if already present.
     */
    public int sadd(String key, String member) {
        LinkedHashSet<String> set = sets.computeIfAbsent(key, k -> new LinkedHashSet<>());
        return set.add(member) ? 1 : 0;
    }

    /**
     * Remove a member from the set. Returns 1 if removed, 0 if not present.
     */
    public int srem(String key, String member) {
        LinkedHashSet<String> set = sets.get(key);
        if (set == null) return 0;
        boolean removed = set.remove(member);
        if (set.isEmpty()) sets.remove(key);
        return removed ? 1 : 0;
    }

    /**
     * Test membership. Returns true if member is in the set.
     */
    public boolean sismember(String key, String member) {
        LinkedHashSet<String> set = sets.get(key);
        return set != null && set.contains(member);
    }

    /**
     * Returns the number of members, or 0 if absent.
     */
    public int scard(String key) {
        LinkedHashSet<String> set = sets.get(key);
        return set == null ? 0 : set.size();
    }

    /**
     * Returns all members of the set, or an empty set if absent.
     */
    public java.util.Set<String> smembers(String key) {
        LinkedHashSet<String> set = sets.get(key);
        return set == null ? Collections.emptySet() : Collections.unmodifiableSet(set);
    }

    // ================================================================
    // Sorted-set operations
    // ================================================================

    /**
     * Add or update a member with its score.
     * Returns 1 if a new member was added, 0 if an existing member was updated.
     */
    public int zadd(String key, double score, String member) {
        TreeMap<Double, String> zset = zsets.computeIfAbsent(key, k -> new TreeMap<>());
        boolean isNew = !zset.containsValue(member);
        zset.put(score, member);
        return isNew ? 1 : 0;
    }

    /**
     * Remove a member by value. Returns 1 if removed, 0 if not present.
     */
    public int zrem(String key, String member) {
        TreeMap<Double, String> zset = zsets.get(key);
        if (zset == null) return 0;
        boolean removed = zset.values().remove(member);
        if (zset.isEmpty()) zsets.remove(key);
        return removed ? 1 : 0;
    }

    /**
     * Return members in score order from rank start to stop (inclusive, 0-based).
     */
    public List<String> zrange(String key, int start, int stop) {
        TreeMap<Double, String> zset = zsets.get(key);
        if (zset == null || zset.isEmpty()) return Collections.emptyList();

        List<String> all  = new ArrayList<>(zset.values());
        int          size = all.size();

        if (start < 0) start = Math.max(0, size + start);
        if (stop  < 0) stop  = size + stop;
        stop = Math.min(stop, size - 1);

        if (start > stop) return Collections.emptyList();
        return Collections.unmodifiableList(all.subList(start, stop + 1));
    }

    /**
     * Returns the score of a member, or null if the key or member does not exist.
     */
    public Double zscore(String key, String member) {
        TreeMap<Double, String> zset = zsets.get(key);
        if (zset == null) return null;
        for (Map.Entry<Double, String> e : zset.entrySet()) {
            if (e.getValue().equals(member)) return e.getKey();
        }
        return null;
    }

    /**
     * Returns the number of members, or 0 if absent.
     */
    public int zcard(String key) {
        TreeMap<Double, String> zset = zsets.get(key);
        return zset == null ? 0 : zset.size();
    }
}
