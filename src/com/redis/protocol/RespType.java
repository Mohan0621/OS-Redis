package com.redis.protocol;

public enum RespType {
    SIMPLE_STRING,
    BULK_STRING,
    INTEGER,
    ERROR,
    NULL,
    ARRAY
}
