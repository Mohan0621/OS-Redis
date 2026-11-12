package com.redis.db;

public class SetOptions {

    public boolean nx;
    public boolean xx;
    public long expirationTimeMillis = -1;
}
