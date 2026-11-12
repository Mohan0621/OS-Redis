package com.redis.db.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Store for Redis List keys, including blocking pop waiter notifications.
 */
public class ListStore {

    private final Map<String, LinkedList<String>> lists = new ConcurrentHashMap<>();
    private final Map<String, ConcurrentLinkedQueue<CompletableFuture<Void>>> listWaiters =
            new ConcurrentHashMap<>();
    private final ExpirationManager expirations;

    public ListStore(ExpirationManager expirations) {
        this.expirations = expirations;
    }

    public boolean containsKey(String key) {
        return lists.containsKey(key);
    }

    public boolean removeRaw(String key) {
        return lists.remove(key) != null;
    }

    private boolean isExpired(String key) {
        return expirations.purgeIfExpired(key, lists::remove);
    }

    public CompletableFuture<Void> registerWaiter(String key) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        registerWaiter(key, future);
        return future;
    }

    public void registerWaiter(String key, CompletableFuture<Void> future) {
        listWaiters.computeIfAbsent(key, k -> new ConcurrentLinkedQueue<>()).add(future);
    }

    public void unregisterWaiter(String key, CompletableFuture<Void> future) {
        ConcurrentLinkedQueue<CompletableFuture<Void>> q = listWaiters.get(key);
        if (q != null) {
            q.remove(future);
            if (q.isEmpty()) {
                listWaiters.remove(key, q);
            }
        }
    }

    private void notifyWaiters(String key) {
        ConcurrentLinkedQueue<CompletableFuture<Void>> q = listWaiters.get(key);
        if (q == null) return;
        CompletableFuture<Void> waiter;
        while ((waiter = q.poll()) != null) {
            waiter.complete(null);
        }
    }

    public int lpush(String key, String... values) {
        isExpired(key);
        int[] size = new int[1];
        lists.compute(key, (k, existing) -> {
            LinkedList<String> list = existing != null ? existing : new LinkedList<>();
            for (String v : values) {
                list.addFirst(v);
            }
            size[0] = list.size();
            return list;
        });
        notifyWaiters(key);
        return size[0];
    }

    public int rpush(String key, String... values) {
        isExpired(key);
        int[] size = new int[1];
        lists.compute(key, (k, existing) -> {
            LinkedList<String> list = existing != null ? existing : new LinkedList<>();
            for (String v : values) {
                list.addLast(v);
            }
            size[0] = list.size();
            return list;
        });
        notifyWaiters(key);
        return size[0];
    }

    public String lpop(String key) {
        if (isExpired(key)) return null;
        String[] popped = new String[1];
        lists.computeIfPresent(key, (k, list) -> {
            if (list.isEmpty()) {
                expirations.clear(k);
                return null;
            }
            popped[0] = list.removeFirst();
            if (list.isEmpty()) {
                expirations.clear(k);
                return null;
            }
            return list;
        });
        return popped[0];
    }

    public String rpop(String key) {
        if (isExpired(key)) return null;
        String[] popped = new String[1];
        lists.computeIfPresent(key, (k, list) -> {
            if (list.isEmpty()) {
                expirations.clear(k);
                return null;
            }
            popped[0] = list.removeLast();
            if (list.isEmpty()) {
                expirations.clear(k);
                return null;
            }
            return list;
        });
        return popped[0];
    }

    public List<String> blpopTry(String... keys) {
        for (String key : keys) {
            String val = lpop(key);
            if (val != null) {
                return List.of(key, val);
            }
        }
        return null;
    }

    public List<String> brpopTry(String... keys) {
        for (String key : keys) {
            String val = rpop(key);
            if (val != null) {
                return List.of(key, val);
            }
        }
        return null;
    }

    public int llen(String key) {
        if (isExpired(key)) return 0;
        LinkedList<String> list = lists.get(key);
        if (list == null) return 0;
        synchronized (list) {
            return list.size();
        }
    }

    public List<String> lrange(String key, int start, int stop) {
        if (isExpired(key)) return Collections.emptyList();
        LinkedList<String> list = lists.get(key);
        if (list == null) return Collections.emptyList();

        List<String> snapshot;
        synchronized (list) {
            if (list.isEmpty()) return Collections.emptyList();
            snapshot = new ArrayList<>(list);
        }

        int size = snapshot.size();
        if (start < 0) start = Math.max(0, size + start);
        if (stop < 0) stop = size + stop;
        stop = Math.min(stop, size - 1);

        if (start > stop) return Collections.emptyList();
        return Collections.unmodifiableList(snapshot.subList(start, stop + 1));
    }

    public String lindex(String key, int index) {
        if (isExpired(key)) return null;
        LinkedList<String> list = lists.get(key);
        if (list == null) return null;
        synchronized (list) {
            int size = list.size();
            int idx = index < 0 ? size + index : index;
            if (idx < 0 || idx >= size) return null;
            return list.get(idx);
        }
    }

    public void lset(String key, int index, String element) {
        if (isExpired(key)) {
            throw new IllegalArgumentException("no such key");
        }
        boolean[] found = new boolean[1];
        lists.computeIfPresent(key, (k, list) -> {
            found[0] = true;
            int size = list.size();
            int idx = index < 0 ? size + index : index;
            if (idx < 0 || idx >= size) {
                throw new IndexOutOfBoundsException("index out of range");
            }
            list.set(idx, element);
            return list;
        });
        if (!found[0]) {
            throw new IllegalArgumentException("no such key");
        }
    }

    public int lrem(String key, int count, String element) {
        if (isExpired(key)) return 0;
        int[] removed = new int[1];
        lists.computeIfPresent(key, (k, list) -> {
            if (count == 0) {
                Iterator<String> it = list.iterator();
                while (it.hasNext()) {
                    if (it.next().equals(element)) {
                        it.remove();
                        removed[0]++;
                    }
                }
            } else if (count > 0) {
                Iterator<String> it = list.iterator();
                while (it.hasNext() && removed[0] < count) {
                    if (it.next().equals(element)) {
                        it.remove();
                        removed[0]++;
                    }
                }
            } else {
                int target = -count;
                Iterator<String> it = list.descendingIterator();
                while (it.hasNext() && removed[0] < target) {
                    if (it.next().equals(element)) {
                        it.remove();
                        removed[0]++;
                    }
                }
            }
            if (list.isEmpty()) {
                expirations.clear(k);
                return null;
            }
            return list;
        });
        return removed[0];
    }

    public int linsert(String key, boolean before, String pivot, String element) {
        if (isExpired(key)) return 0;
        int[] result = new int[] { 0 };
        lists.computeIfPresent(key, (k, list) -> {
            int idx = list.indexOf(pivot);
            if (idx == -1) {
                result[0] = -1;
                return list;
            }
            int insertIdx = before ? idx : idx + 1;
            list.add(insertIdx, element);
            result[0] = list.size();
            return list;
        });
        if (result[0] > 0) {
            notifyWaiters(key);
        }
        return result[0];
    }

    public void ltrim(String key, int start, int stop) {
        if (isExpired(key)) return;
        lists.computeIfPresent(key, (k, list) -> {
            int size = list.size();
            int s = start < 0 ? Math.max(0, size + start) : start;
            int e = stop < 0 ? size + stop : Math.min(stop, size - 1);
            if (s > e || s >= size || e < 0) {
                expirations.clear(k);
                return null;
            }
            List<String> kept = new ArrayList<>(list).subList(s, e + 1);
            return new LinkedList<>(kept);
        });
    }
}

