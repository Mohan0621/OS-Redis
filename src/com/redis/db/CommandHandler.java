package com.redis.db;

import com.redis.db.handler.HashCommandHandler;
import com.redis.db.handler.ListCommandHandler;
import com.redis.db.handler.SetCommandHandler;
import com.redis.db.handler.SortedSetCommandHandler;
import com.redis.db.handler.StringCommandHandler;
import com.redis.protocol.RespCommand;

/**
 * Top-level command dispatcher delegating each Redis command to its
 * data-type-specific handler.
 */
public class CommandHandler {

    private final StringCommandHandler    stringHandler;
    private final ListCommandHandler      listHandler;
    private final HashCommandHandler      hashHandler;
    private final SetCommandHandler       setHandler;
    private final SortedSetCommandHandler zsetHandler;

    public CommandHandler(RedisDatabase database) {
        this.stringHandler = new StringCommandHandler(database);
        this.listHandler   = new ListCommandHandler(database);
        this.hashHandler   = new HashCommandHandler(database);
        this.setHandler    = new SetCommandHandler(database);
        this.zsetHandler   = new SortedSetCommandHandler(database);
    }

    public CommandResult execute(RespCommand command) {
        return execute(command.getArguments());
    }

    public CommandResult execute(String[] command) {

        if (command == null || command.length == 0) {
            return CommandResult.error("ERR empty command");
        }

        String operation = command[0].toUpperCase();

        return switch (operation) {

            // ---- Keyless ----
            case "PING" -> handlePing(command);

            // ---- String / Key / TTL ----
            case "SET", "GET", "DEL", "INCR", "DECR",
                 "MGET", "MSET", "SETNX", "GETSET", "GETDEL", "GETEX",
                 "APPEND", "STRLEN", "TTL", "PTTL", "EXPIRE", "PEXPIRE", "PERSIST"
                    -> stringHandler.handle(operation, command);

            // ---- List ----
            case "LPUSH", "RPUSH", "LPOP", "RPOP", "LLEN", "LRANGE",
                 "LINDEX", "LSET", "LREM", "LINSERT", "LTRIM", "BLPOP", "BRPOP"
                    -> listHandler.handle(operation, command);

            // ---- Hash ----
            case "HSET", "HMSET", "HGET", "HMGET", "HDEL", "HGETALL",
                 "HKEYS", "HVALS", "HEXISTS", "HLEN", "HINCRBY", "HINCRBYFLOAT"
                    -> hashHandler.handle(operation, command);

            // ---- Set ----
            case "SADD", "SREM", "SISMEMBER", "SCARD", "SMEMBERS",
                 "SUNION", "SINTER", "SDIFF", "SUNIONSTORE", "SINTERSTORE", "SDIFFSTORE",
                 "SMOVE", "SRANDMEMBER", "SPOP"
                    -> setHandler.handle(operation, command);

            // ---- Sorted set ----
            case "ZADD", "ZREM", "ZRANGE", "ZREVRANGE", "ZSCORE", "ZCARD",
                 "ZRANK", "ZREVRANK", "ZINCRBY", "ZRANGEBYSCORE", "ZCOUNT",
                 "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE", "ZRANGEBYLEX", "ZLEXCOUNT"
                    -> zsetHandler.handle(operation, command);

            default -> CommandResult.error("ERR unknown command '" + command[0] + "'");
        };
    }

    private CommandResult handlePing(String[] command) {
        if (command.length == 1) return CommandResult.simpleString("PONG");
        if (command.length == 2) return CommandResult.bulkString(command[1]);
        return CommandResult.error("ERR wrong number of arguments for 'ping'");
    }
}
