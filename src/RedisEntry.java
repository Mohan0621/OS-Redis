public class RedisEntry {

    private final String value;
    private final long expirationTimeMillis;

    public RedisEntry(
            String value,
            long expirationTimeMillis) {

        this.value = value;
        this.expirationTimeMillis =
                expirationTimeMillis;
    }

    public String getValue() {
        return value;
    }
    public long getExpirationTimeMillis() {
        return expirationTimeMillis;
    }
    public boolean isExpired() {

        return expirationTimeMillis != -1
                && System.currentTimeMillis()
                >= expirationTimeMillis;
    }
}