import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

public class RedisDatabase {

    private final Map<String, String> data = new ConcurrentHashMap<>();

    public void set(String key, String value) {
        data.put(key, value);
    }

    public String get(String key) {
        return data.get(key);
    }

    public String delete(String key) {   // returns the old value, or null if absent
        return data.remove(key);
    }
}
