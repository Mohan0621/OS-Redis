package com.redis.db;

import java.util.List;
import java.util.Map;
import com.redis.protocol.RespCommand;

public class CommandHandler {

    private final RedisDatabase database;

    public CommandHandler(RedisDatabase database) {
        this.database = database;
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
            case "PING"    -> handlePing(command);

            // ---- String ----
            case "SET"     -> handleSet(command);
            case "GET"     -> handleGet(command);
            case "DEL"     -> handleDelete(command);
            case "INCR"    -> handleIncrement(command);
            case "TTL"     -> handleTtl(command);

            // ---- List ----
            case "LPUSH"   -> handleLpush(command);
            case "RPUSH"   -> handleRpush(command);
            case "LPOP"    -> handleLpop(command);
            case "RPOP"    -> handleRpop(command);
            case "LLEN"    -> handleLlen(command);
            case "LRANGE"  -> handleLrange(command);

            // ---- Hash ----
            case "HSET"    -> handleHset(command);
            case "HGET"    -> handleHget(command);
            case "HDEL"    -> handleHdel(command);
            case "HGETALL" -> handleHgetall(command);
            case "HEXISTS" -> handleHexists(command);
            case "HLEN"    -> handleHlen(command);

            // ---- Set ----
            case "SADD"      -> handleSadd(command);
            case "SREM"      -> handleSrem(command);
            case "SISMEMBER" -> handleSismember(command);
            case "SCARD"     -> handleScard(command);
            case "SMEMBERS"  -> handleSmembers(command);

            // ---- Sorted set ----
            case "ZADD"   -> handleZadd(command);
            case "ZREM"   -> handleZrem(command);
            case "ZRANGE" -> handleZrange(command);
            case "ZSCORE" -> handleZscore(command);
            case "ZCARD"  -> handleZcard(command);

            default -> CommandResult.error("ERR unknown command '" + command[0] + "'");
        };
    }

    // ================================================================
    // Keyless
    // ================================================================

    private CommandResult handlePing(String[] command) {
        if (command.length == 1)   return CommandResult.simpleString("PONG");
        return CommandResult.bulkString(command[1]);   // PING <message>
    }

    // ================================================================
    // String
    // ================================================================

    private CommandResult handleSet(String[] command) {

        if (command.length < 3) {
            return CommandResult.error("ERR wrong number of arguments for 'set'");
        }

        String key   = command[1];
        String value = command[2];

        try {
            SetOptions options = parseSetOptions(command);
            boolean success;

            if (options.nx) {
                success = database.setIfAbsent(key, value, options.expirationTimeMillis);
            } else if (options.xx) {
                success = database.setIfPresent(key, value, options.expirationTimeMillis);
            } else {
                database.set(key, value, options.expirationTimeMillis);
                success = true;
            }

            return success ? CommandResult.simpleString("OK") : CommandResult.nil();

        } catch (IllegalArgumentException e) {
            return CommandResult.error("ERR " + e.getMessage());
        }
    }

    private CommandResult handleGet(String[] command) {
        if (command.length != 2) {
            return CommandResult.error("ERR wrong number of arguments for 'get'");
        }
        String value = database.get(command[1]);
        return value == null ? CommandResult.nil() : CommandResult.bulkString(value);
    }

    private CommandResult handleDelete(String[] command) {
        if (command.length != 2) {
            return CommandResult.error("ERR wrong number of arguments for 'del'");
        }
        String deleted = database.delete(command[1]);
        return CommandResult.integer(deleted != null ? 1 : 0);
    }

    private CommandResult handleIncrement(String[] command) {
        if (command.length != 2) {
            return CommandResult.error("ERR wrong number of arguments for 'incr'");
        }
        try {
            String value = database.increment(command[1]);
            return CommandResult.integer(Integer.parseInt(value));
        } catch (NumberFormatException e) {
            return CommandResult.error("ERR value is not an integer or out of range");
        }
    }

    private CommandResult handleTtl(String[] command) {
        if (command.length != 2) {
            return CommandResult.error("ERR wrong number of arguments for 'ttl'");
        }
        long ttl = database.ttl(command[1]);
        return CommandResult.integer((int) ttl);
    }

    // ================================================================
    // List
    // ================================================================

    private CommandResult handleLpush(String[] command) {
        if (command.length != 3) {
            return CommandResult.error("ERR wrong number of arguments for 'lpush'");
        }
        int len = database.lpush(command[1], command[2]);
        return CommandResult.integer(len);
    }

    private CommandResult handleRpush(String[] command) {
        if (command.length != 3) {
            return CommandResult.error("ERR wrong number of arguments for 'rpush'");
        }
        int len = database.rpush(command[1], command[2]);
        return CommandResult.integer(len);
    }

    private CommandResult handleLpop(String[] command) {
        if (command.length != 2) {
            return CommandResult.error("ERR wrong number of arguments for 'lpop'");
        }
        String value = database.lpop(command[1]);
        return value == null ? CommandResult.nil() : CommandResult.bulkString(value);
    }

    private CommandResult handleRpop(String[] command) {
        if (command.length != 2) {
            return CommandResult.error("ERR wrong number of arguments for 'rpop'");
        }
        String value = database.rpop(command[1]);
        return value == null ? CommandResult.nil() : CommandResult.bulkString(value);
    }

    private CommandResult handleLlen(String[] command) {
        if (command.length != 2) {
            return CommandResult.error("ERR wrong number of arguments for 'llen'");
        }
        return CommandResult.integer(database.llen(command[1]));
    }

    private CommandResult handleLrange(String[] command) {
        if (command.length != 4) {
            return CommandResult.error("ERR wrong number of arguments for 'lrange'");
        }
        try {
            int start = Integer.parseInt(command[2]);
            int stop  = Integer.parseInt(command[3]);
            List<String> items = database.lrange(command[1], start, stop);
            return buildArrayResult(items);
        } catch (NumberFormatException e) {
            return CommandResult.error("ERR value is not an integer or out of range");
        }
    }

    // ================================================================
    // Hash
    // ================================================================

    private CommandResult handleHset(String[] command) {
        if (command.length != 4) {
            return CommandResult.error("ERR wrong number of arguments for 'hset'");
        }
        int added = database.hset(command[1], command[2], command[3]);
        return CommandResult.integer(added);
    }

    private CommandResult handleHget(String[] command) {
        if (command.length != 3) {
            return CommandResult.error("ERR wrong number of arguments for 'hget'");
        }
        String value = database.hget(command[1], command[2]);
        return value == null ? CommandResult.nil() : CommandResult.bulkString(value);
    }

    private CommandResult handleHdel(String[] command) {
        if (command.length != 3) {
            return CommandResult.error("ERR wrong number of arguments for 'hdel'");
        }
        int removed = database.hdel(command[1], command[2]);
        return CommandResult.integer(removed);
    }

    private CommandResult handleHgetall(String[] command) {
        if (command.length != 2) {
            return CommandResult.error("ERR wrong number of arguments for 'hgetall'");
        }
        Map<String, String> hash = database.hgetall(command[1]);
        // HGETALL returns alternating field, value entries as an array
        List<String> flat = new java.util.ArrayList<>(hash.size() * 2);
        for (Map.Entry<String, String> e : hash.entrySet()) {
            flat.add(e.getKey());
            flat.add(e.getValue());
        }
        return buildArrayResult(flat);
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

    // ================================================================
    // Set
    // ================================================================

    private CommandResult handleSadd(String[] command) {
        if (command.length != 3) {
            return CommandResult.error("ERR wrong number of arguments for 'sadd'");
        }
        return CommandResult.integer(database.sadd(command[1], command[2]));
    }

    private CommandResult handleSrem(String[] command) {
        if (command.length != 3) {
            return CommandResult.error("ERR wrong number of arguments for 'srem'");
        }
        return CommandResult.integer(database.srem(command[1], command[2]));
    }

    private CommandResult handleSismember(String[] command) {
        if (command.length != 3) {
            return CommandResult.error("ERR wrong number of arguments for 'sismember'");
        }
        return CommandResult.integer(database.sismember(command[1], command[2]) ? 1 : 0);
    }

    private CommandResult handleScard(String[] command) {
        if (command.length != 2) {
            return CommandResult.error("ERR wrong number of arguments for 'scard'");
        }
        return CommandResult.integer(database.scard(command[1]));
    }

    private CommandResult handleSmembers(String[] command) {
        if (command.length != 2) {
            return CommandResult.error("ERR wrong number of arguments for 'smembers'");
        }
        return buildArrayResult(new java.util.ArrayList<>(database.smembers(command[1])));
    }

    // ================================================================
    // Sorted set
    // ================================================================

    private CommandResult handleZadd(String[] command) {
        if (command.length != 4) {
            return CommandResult.error("ERR wrong number of arguments for 'zadd'");
        }
        try {
            double score = Double.parseDouble(command[2]);
            int added = database.zadd(command[1], score, command[3]);
            return CommandResult.integer(added);
        } catch (NumberFormatException e) {
            return CommandResult.error("ERR value is not a valid float");
        }
    }

    private CommandResult handleZrem(String[] command) {
        if (command.length != 3) {
            return CommandResult.error("ERR wrong number of arguments for 'zrem'");
        }
        return CommandResult.integer(database.zrem(command[1], command[2]));
    }

    private CommandResult handleZrange(String[] command) {
        if (command.length != 4) {
            return CommandResult.error("ERR wrong number of arguments for 'zrange'");
        }
        try {
            int start = Integer.parseInt(command[2]);
            int stop  = Integer.parseInt(command[3]);
            List<String> members = database.zrange(command[1], start, stop);
            return buildArrayResult(members);
        } catch (NumberFormatException e) {
            return CommandResult.error("ERR value is not an integer or out of range");
        }
    }

    private CommandResult handleZscore(String[] command) {
        if (command.length != 3) {
            return CommandResult.error("ERR wrong number of arguments for 'zscore'");
        }
        Double score = database.zscore(command[1], command[2]);
        return score == null ? CommandResult.nil() : CommandResult.bulkString(String.valueOf(score));
    }

    private CommandResult handleZcard(String[] command) {
        if (command.length != 2) {
            return CommandResult.error("ERR wrong number of arguments for 'zcard'");
        }
        return CommandResult.integer(database.zcard(command[1]));
    }

    // ================================================================
    // Internal helpers
    // ================================================================

    /**
     * Build a RESP array result from a list of strings.
     * Each element is encoded as a bulk string by RespWriter.
     * An empty list returns an empty RESP array (*0\r\n).
     */
    private static CommandResult buildArrayResult(List<String> items) {
        return CommandResult.array(items);
    }

    private SetOptions parseSetOptions(String[] command) {

        SetOptions options = new SetOptions();
        int index = 3;

        while (index < command.length) {

            String option = command[index].toUpperCase();

            switch (option) {

                case "NX":
                    if (options.xx) throw new IllegalArgumentException("NX and XX are mutually exclusive");
                    options.nx = true;
                    index++;
                    break;

                case "XX":
                    if (options.nx) throw new IllegalArgumentException("NX and XX are mutually exclusive");
                    options.xx = true;
                    index++;
                    break;

                case "EX":
                    if (index + 1 >= command.length)
                        throw new IllegalArgumentException("EX requires a value");
                    long seconds;
                    try {
                        seconds = Long.parseLong(command[index + 1]);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("invalid expire time");
                    }
                    if (seconds <= 0) throw new IllegalArgumentException("invalid expire time");
                    options.expirationTimeMillis = System.currentTimeMillis() + seconds * 1000;
                    index += 2;
                    break;

                default:
                    throw new IllegalArgumentException("syntax error");
            }
        }

        return options;
    }
}
