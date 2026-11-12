package com.redis.db;

import java.util.Collections;
import java.util.List;
import com.redis.protocol.RespType;

public class CommandResult {

    private final RespType    type;
    private final String      value;     // used by all types except ARRAY
    private final List<String> elements; // used only by ARRAY

    private CommandResult(RespType type, String value) {
        this.type     = type;
        this.value    = value;
        this.elements = null;
    }

    private CommandResult(List<String> elements) {
        this.type     = RespType.ARRAY;
        this.value    = null;
        this.elements = Collections.unmodifiableList(elements);
    }

    // ---- Factories ----

    public static CommandResult simpleString(String value) {
        return new CommandResult(RespType.SIMPLE_STRING, value);
    }

    public static CommandResult bulkString(String value) {
        return new CommandResult(RespType.BULK_STRING, value);
    }

    public static CommandResult integer(int value) {
        return new CommandResult(RespType.INTEGER, String.valueOf(value));
    }

    public static CommandResult integer(long value) {
        return new CommandResult(RespType.INTEGER, String.valueOf(value));
    }

    public static CommandResult error(String message) {
        return new CommandResult(RespType.ERROR, message);
    }

    public static CommandResult nil() {
        return new CommandResult(RespType.NULL, null);
    }

    /**
     * RESP array of bulk strings.
     * A null element inside the list encodes as $-1 (nil bulk string).
     */
    public static CommandResult array(List<String> elements) {
        return new CommandResult(elements);
    }

    // ---- Accessors ----

    public RespType getType() {
        return type;
    }

    public String getValue() {
        return value;
    }

    /**
     * Non-null only when getType() == ARRAY.
     */
    public List<String> getElements() {
        return elements;
    }
}
