import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

public class RedisDatabase {

    private final Map<String, RedisEntry> data = new ConcurrentHashMap<>();
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
}
