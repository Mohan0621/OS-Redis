package com.redis.db.handler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import com.redis.db.CommandResult;
import com.redis.db.RedisDatabase;

/**
 * Handles Redis Hash commands.
 */
public class HashCommandHandler {

    private final RedisDatabase database;

    public HashCommandHandler(RedisDatabase database) {
        this.database = database;
    }

    public CommandResult handle(String operation, String[] command) {
        return switch (operation) {
            case "HSET"         -> handleHset(command);
            case "HMSET"        -> handleHmset(command);
            case "HGET"         -> handleHget(command);
            case "HMGET"        -> handleHmget(command);
            case "HDEL"         -> handleHdel(command);
            case "HGETALL"      -> handleHgetall(command);
            case "HKEYS"        -> handleHkeys(command);
            case "HVALS"        -> handleHvals(command);
            case "HEXISTS"      -> handleHexists(command);
            case "HLEN"         -> handleHlen(command);
            case "HINCRBY"      -> handleHincrby(command);
            case "HINCRBYFLOAT" -> handleHincrbyfloat(command);
            default -> CommandResult.error("ERR unknown command '" + command[0] + "'");
        };
    }

    private CommandResult handleHset(String[] command) {
        if (command.length < 4 || (command.length - 2) % 2 != 0) {
            return CommandResult.error("ERR wrong number of arguments for 'hset'");
        }
        if (command.length == 4) {
            return CommandResult.integer(database.hset(command[1], command[2], command[3]));
        }
        return CommandResult.integer(database.hsetMultiple(command[1], command, 2));
    }

    private CommandResult handleHmset(String[] command) {
        if (command.length < 4 || (command.length - 2) % 2 != 0) {
            return CommandResult.error("ERR wrong number of arguments for 'hmset'");
        }
        database.hsetMultiple(command[1], command, 2);
        return CommandResult.simpleString("OK");
    }

    private CommandResult handleHget(String[] command) {
        if (command.length != 3) {
            return CommandResult.error("ERR wrong number of arguments for 'hget'");
        }
        String value = database.hget(command[1], command[2]);
        return value == null ? CommandResult.nil() : CommandResult.bulkString(value);
    }

    private CommandResult handleHmget(String[] command) {
        if (command.length < 3) {
            return CommandResult.error("ERR wrong number of arguments for 'hmget'");
        }
        String[] fields = Arrays.copyOfRange(command, 2, command.length);
        return CommandResult.array(database.hmget(command[1], fields));
    }

    private CommandResult handleHdel(String[] command) {
        if (command.length < 3) {
            return CommandResult.error("ERR wrong number of arguments for 'hdel'");
        }
        String[] fields = Arrays.copyOfRange(command, 2, command.length);
        return CommandResult.integer(database.hdel(command[1], fields));
    }

    private CommandResult handleHgetall(String[] command) {
        if (command.length != 2) {
            return CommandResult.error("ERR wrong number of arguments for 'hgetall'");
        }
        Map<String, String> hash = database.hgetall(command[1]);
        List<String> flat = new ArrayList<>(hash.size() * 2);
        for (Map.Entry<String, String> e : hash.entrySet()) {
            flat.add(e.getKey());
            flat.add(e.getValue());
        }
        return CommandResult.array(flat);
    }

    private CommandResult handleHkeys(String[] command) {
        if (command.length != 2) {
            return CommandResult.error("ERR wrong number of arguments for 'hkeys'");
        }
        return CommandResult.array(database.hkeys(command[1]));
    }

    private CommandResult handleHvals(String[] command) {
        if (command.length != 2) {
            return CommandResult.error("ERR wrong number of arguments for 'hvals'");
        }
        return CommandResult.array(database.hvals(command[1]));
    }

    private CommandResult handleHexists(String[] command) {
        if (command.length != 3) {
            return CommandResult.error("ERR wrong number of arguments for 'hexists'");
        }
        return CommandResult.integer(database.hexists(command[1], command[2]) ? 1 : 0);
    }

    private CommandResult handleHlen(String[] command) {
        if (command.length != 2) {
            return CommandResult.error("ERR wrong number of arguments for 'hlen'");
        }
        return CommandResult.integer(database.hlen(command[1]));
    }

    private CommandResult handleHincrby(String[] command) {
        if (command.length != 4) {
            return CommandResult.error("ERR wrong number of arguments for 'hincrby'");
        }
        try {
            long increment = Long.parseLong(command[3]);
            return CommandResult.integer(database.hincrby(command[1], command[2], increment));
        } catch (NumberFormatException | ArithmeticException e) {
            return CommandResult.error("ERR hash value is not an integer or out of range");
        }
    }

    private CommandResult handleHincrbyfloat(String[] command) {
        if (command.length != 4) {
            return CommandResult.error("ERR wrong number of arguments for 'hincrbyfloat'");
        }
        try {
            String value = database.hincrbyfloat(command[1], command[2], command[3]);
            return CommandResult.bulkString(value);
        } catch (IllegalArgumentException e) {
            return CommandResult.error("ERR hash value or increment is not a valid float");
        }
    }
}

