package com.redis.db.handler;

import java.util.Arrays;
import com.redis.db.CommandResult;
import com.redis.db.RedisDatabase;
import com.redis.db.SetOptions;

/**
 * Handles String and general key/TTL commands.
 */
public class StringCommandHandler {

    private final RedisDatabase database;

    public StringCommandHandler(RedisDatabase database) {
        this.database = database;
    }

    public CommandResult handle(String operation, String[] command) {
        return switch (operation) {
            case "SET"     -> handleSet(command);
            case "GET"     -> handleGet(command);
            case "DEL"     -> handleDelete(command);
            case "INCR"    -> handleIncrement(command);
            case "DECR"    -> handleDecrement(command);
            case "MGET"    -> handleMget(command);
            case "MSET"    -> handleMset(command);
            case "SETNX"   -> handleSetnx(command);
            case "GETSET"  -> handleGetset(command);
            case "GETDEL"  -> handleGetdel(command);
            case "GETEX"   -> handleGetex(command);
            case "APPEND"  -> handleAppend(command);
            case "STRLEN"  -> handleStrlen(command);
            case "TTL"     -> handleTtl(command);
            case "PTTL"    -> handlePttl(command);
            case "EXPIRE"  -> handleExpire(command);
            case "PEXPIRE" -> handlePexpire(command);
            case "PERSIST" -> handlePersist(command);
            default -> CommandResult.error("ERR unknown command '" + command[0] + "'");
        };
    }

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
        if (command.length < 2) {
            return CommandResult.error("ERR wrong number of arguments for 'del'");
        }
        String[] keys = Arrays.copyOfRange(command, 1, command.length);
        return CommandResult.integer(database.del(keys));
    }

    private CommandResult handleIncrement(String[] command) {
        if (command.length != 2) {
            return CommandResult.error("ERR wrong number of arguments for 'incr'");
        }
        try {
            String value = database.increment(command[1]);
            return CommandResult.integer(Long.parseLong(value));
        } catch (NumberFormatException | ArithmeticException e) {
            return CommandResult.error("ERR value is not an integer or out of range");
        }
    }

    private CommandResult handleDecrement(String[] command) {
        if (command.length != 2) {
            return CommandResult.error("ERR wrong number of arguments for 'decr'");
        }
        try {
            String value = database.decrement(command[1]);
            return CommandResult.integer(Long.parseLong(value));
        } catch (NumberFormatException | ArithmeticException e) {
            return CommandResult.error("ERR value is not an integer or out of range");
        }
    }

    private CommandResult handleMget(String[] command) {
        if (command.length < 2) {
            return CommandResult.error("ERR wrong number of arguments for 'mget'");
        }
        String[] keys = Arrays.copyOfRange(command, 1, command.length);
        return CommandResult.array(database.mget(keys));
    }

    private CommandResult handleMset(String[] command) {
        if (command.length < 3 || (command.length - 1) % 2 != 0) {
            return CommandResult.error("ERR wrong number of arguments for 'mset'");
        }
        String[] pairs = Arrays.copyOfRange(command, 1, command.length);
        database.mset(pairs);
        return CommandResult.simpleString("OK");
    }

    private CommandResult handleSetnx(String[] command) {
        if (command.length != 3) {
            return CommandResult.error("ERR wrong number of arguments for 'setnx'");
        }
        boolean added = database.setIfAbsent(command[1], command[2], -1L);
        return CommandResult.integer(added ? 1 : 0);
    }

    private CommandResult handleGetset(String[] command) {
        if (command.length != 3) {
            return CommandResult.error("ERR wrong number of arguments for 'getset'");
        }
        String prev = database.getSet(command[1], command[2]);
        return prev == null ? CommandResult.nil() : CommandResult.bulkString(prev);
    }

    private CommandResult handleGetdel(String[] command) {
        if (command.length != 2) {
            return CommandResult.error("ERR wrong number of arguments for 'getdel'");
        }
        String value = database.getDel(command[1]);
        return value == null ? CommandResult.nil() : CommandResult.bulkString(value);
    }

    private CommandResult handleGetex(String[] command) {
        if (command.length < 2) {
            return CommandResult.error("ERR wrong number of arguments for 'getex'");
        }
        String key = command[1];
        long expirationMillis = -1L;
        boolean persist = false;
        boolean updateExpiry = false;

        int idx = 2;
        if (idx < command.length) {
            String opt = command[idx].toUpperCase();
            switch (opt) {
                case "PERSIST":
                    if (idx + 1 != command.length) {
                        return CommandResult.error("ERR syntax error");
                    }
                    persist = true;
                    break;
                case "EX":
                case "PX":
                case "EXAT":
                case "PXAT":
                    if (idx + 2 != command.length) {
                        return CommandResult.error("ERR syntax error");
                    }
                    long num;
                    try {
                        num = Long.parseLong(command[idx + 1]);
                    } catch (NumberFormatException e) {
                        return CommandResult.error("ERR value is not an integer or out of range");
                    }
                    if (("EX".equals(opt) || "PX".equals(opt)) && num <= 0) {
                        return CommandResult.error("ERR invalid expire time in 'getex' command");
                    }
                    if ("EX".equals(opt)) {
                        expirationMillis = System.currentTimeMillis() + num * 1000L;
                    } else if ("PX".equals(opt)) {
                        expirationMillis = System.currentTimeMillis() + num;
                    } else if ("EXAT".equals(opt)) {
                        expirationMillis = num * 1000L;
                    } else {
                        expirationMillis = num;
                    }
                    updateExpiry = true;
                    break;
                default:
                    return CommandResult.error("ERR syntax error");
            }
        }

        String value = database.getEx(key, expirationMillis, persist, updateExpiry);
        return value == null ? CommandResult.nil() : CommandResult.bulkString(value);
    }

    private CommandResult handleAppend(String[] command) {
        if (command.length != 3) {
            return CommandResult.error("ERR wrong number of arguments for 'append'");
        }
        return CommandResult.integer(database.append(command[1], command[2]));
    }

    private CommandResult handleStrlen(String[] command) {
        if (command.length != 2) {
            return CommandResult.error("ERR wrong number of arguments for 'strlen'");
        }
        return CommandResult.integer(database.strlen(command[1]));
    }

    private CommandResult handleTtl(String[] command) {
        if (command.length != 2) {
            return CommandResult.error("ERR wrong number of arguments for 'ttl'");
        }
        return CommandResult.integer(database.ttl(command[1]));
    }

    private CommandResult handlePttl(String[] command) {
        if (command.length != 2) {
            return CommandResult.error("ERR wrong number of arguments for 'pttl'");
        }
        return CommandResult.integer(database.pttl(command[1]));
    }

    private CommandResult handleExpire(String[] command) {
        if (command.length != 3) {
            return CommandResult.error("ERR wrong number of arguments for 'expire'");
        }
        try {
            long seconds = Long.parseLong(command[2]);
            return CommandResult.integer(database.expire(command[1], seconds));
        } catch (NumberFormatException e) {
            return CommandResult.error("ERR value is not an integer or out of range");
        }
    }

    private CommandResult handlePexpire(String[] command) {
        if (command.length != 3) {
            return CommandResult.error("ERR wrong number of arguments for 'pexpire'");
        }
        try {
            long millis = Long.parseLong(command[2]);
            return CommandResult.integer(database.pexpire(command[1], millis));
        } catch (NumberFormatException e) {
            return CommandResult.error("ERR value is not an integer or out of range");
        }
    }

    private CommandResult handlePersist(String[] command) {
        if (command.length != 2) {
            return CommandResult.error("ERR wrong number of arguments for 'persist'");
        }
        return CommandResult.integer(database.persist(command[1]));
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
                case "PX":
                    if (index + 1 >= command.length)
                        throw new IllegalArgumentException(option + " requires a value");
                    long duration;
                    try {
                        duration = Long.parseLong(command[index + 1]);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("invalid expire time");
                    }
                    if (duration <= 0) throw new IllegalArgumentException("invalid expire time");
                    long millis = "EX".equals(option) ? duration * 1000L : duration;
                    options.expirationTimeMillis = System.currentTimeMillis() + millis;
                    index += 2;
                    break;

                default:
                    throw new IllegalArgumentException("syntax error");
            }
        }

        return options;
    }
}

