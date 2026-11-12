package com.redis.db.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Store for Redis Set keys.
 */
public class SetStore {

    private final Map<String, LinkedHashSet<String>> sets = new ConcurrentHashMap<>();
    private final ExpirationManager expirations;

    public SetStore(ExpirationManager expirations) {
        this.expirations = expirations;
    }

    public boolean containsKey(String key) {
        return sets.containsKey(key);
    }

    public boolean removeRaw(String key) {
        return sets.remove(key) != null;
    }

    private boolean isExpired(String key) {
        return expirations.purgeIfExpired(key, sets::remove);
    }

    public int sadd(String key, String... members) {
        isExpired(key);
        int[] added = new int[1];
        sets.compute(key, (k, existing) -> {
            LinkedHashSet<String> set = existing != null ? existing : new LinkedHashSet<>();
            for (String m : members) {
                if (set.add(m)) {
                    added[0]++;
                }
            }
            return set;
        });
        return added[0];
    }

    public int srem(String key, String... members) {
        if (isExpired(key))
            return 0;
        int[] removed = new int[1];
        sets.computeIfPresent(key, (k, set) -> {
            for (String m : members) {
                if (set.remove(m)) {
                    removed[0]++;
                }
            }
            if (set.isEmpty()) {
                expirations.clear(k);
                return null;
            }
            return set;
        });
        return removed[0];
    }

    public boolean sismember(String key, String member) {
        if (isExpired(key))
            return false;
        LinkedHashSet<String> set = sets.get(key);
        if (set == null)
            return false;
        synchronized (set) {
            return set.contains(member);
        }
    }

    public int scard(String key) {
        if (isExpired(key))
            return 0;
        LinkedHashSet<String> set = sets.get(key);
        if (set == null)
            return 0;
        synchronized (set) {
            return set.size();
        }
    }

    public Set<String> smembers(String key) {
        if (isExpired(key))
            return Collections.emptySet();
        LinkedHashSet<String> set = sets.get(key);
        if (set == null)
            return Collections.emptySet();
        synchronized (set) {
            return Collections.unmodifiableSet(new LinkedHashSet<>(set));
        }
    }

    private LinkedHashSet<String> snapshotSet(String key) {
        if (isExpired(key))
            return new LinkedHashSet<>();
        LinkedHashSet<String> set = sets.get(key);
        if (set == null)
            return new LinkedHashSet<>();
        synchronized (set) {
            return new LinkedHashSet<>(set);
        }
    }

    public Set<String> sunion(String... keys) {
        LinkedHashSet<String> result = new LinkedHashSet<>();
        for (String key : keys) {
            result.addAll(snapshotSet(key));
        }
        return result;
    }

    public Set<String> sinter(String... keys) {
        if (keys.length == 0)
            return Collections.emptySet();
        LinkedHashSet<String> result = snapshotSet(keys[0]);
        for (int i = 1; i < keys.length && !result.isEmpty(); i++) {
            result.retainAll(snapshotSet(keys[i]));
        }
        return result;
    }

    public Set<String> sdiff(String... keys) {
        if (keys.length == 0)
            return Collections.emptySet();
        LinkedHashSet<String> result = snapshotSet(keys[0]);
        for (int i = 1; i < keys.length && !result.isEmpty(); i++) {
            result.removeAll(snapshotSet(keys[i]));
        }
        return result;
    }

    private int storeSet(String destination, Set<String> values) {
        expirations.clear(destination);
        if (values.isEmpty()) {
            sets.remove(destination);
            return 0;
        }
        sets.put(destination, new LinkedHashSet<>(values));
        return values.size();
    }

    public int sunionstore(String destination, String... keys) {
        return storeSet(destination, sunion(keys));
    }

    public int sinterstore(String destination, String... keys) {
        return storeSet(destination, sinter(keys));
    }

    public int sdiffstore(String destination, String... keys) {
        return storeSet(destination, sdiff(keys));
    }

    public int smove(String source, String destination, String member) {
        if (srem(source, member) == 0) {
            return 0;
        }
        sadd(destination, member);
        return 1;
    }

    public String srandmember(String key) {
        List<String> list = srandmember(key, 1);
        return list.isEmpty() ? null : list.get(0);
    }

    public List<String> srandmember(String key, int count) {
        if (count == 0 || isExpired(key))
            return Collections.emptyList();
        LinkedHashSet<String> set = sets.get(key);
        if (set == null)
            return Collections.emptyList();

        List<String> snapshot;
        synchronized (set) {
            if (set.isEmpty())
                return Collections.emptyList();
            snapshot = new ArrayList<>(set);
        }

        ThreadLocalRandom rng = ThreadLocalRandom.current();
        if (count > 0) {
            Collections.shuffle(snapshot, rng);
            return count >= snapshot.size() ? snapshot : snapshot.subList(0, count);
        } else {
            int absCount = -count;
            List<String> result = new ArrayList<>(absCount);
            for (int i = 0; i < absCount; i++) {
                result.add(snapshot.get(rng.nextInt(snapshot.size())));
            }
            return result;
        }
    }

    public String spop(String key) {
        List<String> popped = spop(key, 1);
        return popped.isEmpty() ? null : popped.get(0);
    }

    public List<String> spop(String key, int count) {
        if (count <= 0 || isExpired(key))
            return Collections.emptyList();
        List<String> popped = new ArrayList<>();
        sets.computeIfPresent(key, (k, set) -> {
            List<String> items = new ArrayList<>(set);
            Collections.shuffle(items, ThreadLocalRandom.current());
            int n = Math.min(count, items.size());
            for (int i = 0; i < n; i++) {
                String m = items.get(i);
                set.remove(m);
                popped.add(m);
            }
            if (set.isEmpty()) {
                expirations.clear(k);
                return null;
            }
            return set;
        });
        return popped;
    }
}
