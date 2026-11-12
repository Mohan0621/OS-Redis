package com.redis.db.handler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import com.redis.db.CommandResult;
import com.redis.db.RedisDatabase;

/**
 * Handles Redis List commands.
 */
public class ListCommandHandler {

    private final RedisDatabase database;

    public ListCommandHandler(RedisDatabase database) {
        this.database = database;
    }

    public CommandResult handle(String operation, String[] command) {
        return switch (operation) {
            case "LPUSH"   -> handleLpush(command);
            case "RPUSH"   -> handleRpush(command);
            case "LPOP"    -> handleLpop(command);
            case "RPOP"    -> handleRpop(command);
            case "LLEN"    -> handleLlen(command);
            case "LRANGE"  -> handleLrange(command);
            case "LINDEX"  -> handleLindex(command);
            case "LSET"    -> handleLset(command);
            case "LREM"    -> handleLrem(command);
            case "LINSERT" -> handleLinsert(command);
            case "LTRIM"   -> handleLtrim(command);
            case "BLPOP"   -> handleBlockingPop(command, true);
            case "BRPOP"   -> handleBlockingPop(command, false);
            default -> CommandResult.error("ERR unknown command '" + command[0] + "'");
        };
    }

    private CommandResult handleLpush(String[] command) {
        if (command.length < 3) {
            return CommandResult.error("ERR wrong number of arguments for 'lpush'");
        }
        String[] values = Arrays.copyOfRange(command, 2, command.length);
        return CommandResult.integer(database.lpush(command[1], values));
    }

    private CommandResult handleRpush(String[] command) {
        if (command.length < 3) {
            return CommandResult.error("ERR wrong number of arguments for 'rpush'");
        }
        String[] values = Arrays.copyOfRange(command, 2, command.length);
        return CommandResult.integer(database.rpush(command[1], values));
    }

    private CommandResult handleLpop(String[] command) {
        if (command.length == 2) {
            String value = database.lpop(command[1]);
            return value == null ? CommandResult.nil() : CommandResult.bulkString(value);
        }
        if (command.length == 3) {
            try {
                int count = Integer.parseInt(command[2]);
                if (count < 0) return CommandResult.error("ERR value is out of range, must be positive");
                List<String> popped = new ArrayList<>();
                for (int i = 0; i < count; i++) {
                    String v = database.lpop(command[1]);
                    if (v == null) break;
                    popped.add(v);
                }
                return popped.isEmpty() ? CommandResult.nil() : CommandResult.array(popped);
            } catch (NumberFormatException e) {
                return CommandResult.error("ERR value is not an integer or out of range");
            }
        }
        return CommandResult.error("ERR wrong number of arguments for 'lpop'");
    }

    private CommandResult handleRpop(String[] command) {
        if (command.length == 2) {
            String value = database.rpop(command[1]);
            return value == null ? CommandResult.nil() : CommandResult.bulkString(value);
        }
        if (command.length == 3) {
            try {
                int count = Integer.parseInt(command[2]);
                if (count < 0) return CommandResult.error("ERR value is out of range, must be positive");
                List<String> popped = new ArrayList<>();
                for (int i = 0; i < count; i++) {
                    String v = database.rpop(command[1]);
                    if (v == null) break;
                    popped.add(v);
                }
                return popped.isEmpty() ? CommandResult.nil() : CommandResult.array(popped);
            } catch (NumberFormatException e) {
                return CommandResult.error("ERR value is not an integer or out of range");
            }
        }
        return CommandResult.error("ERR wrong number of arguments for 'rpop'");
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
            return CommandResult.array(database.lrange(command[1], start, stop));
        } catch (NumberFormatException e) {
            return CommandResult.error("ERR value is not an integer or out of range");
        }
    }

    private CommandResult handleLindex(String[] command) {
        if (command.length != 3) {
            return CommandResult.error("ERR wrong number of arguments for 'lindex'");
        }
        try {
            int index = Integer.parseInt(command[2]);
            String val = database.lindex(command[1], index);
            return val == null ? CommandResult.nil() : CommandResult.bulkString(val);
        } catch (NumberFormatException e) {
            return CommandResult.error("ERR value is not an integer or out of range");
        }
    }

    private CommandResult handleLset(String[] command) {
        if (command.length != 4) {
            return CommandResult.error("ERR wrong number of arguments for 'lset'");
        }
        try {
            int index = Integer.parseInt(command[2]);
            database.lset(command[1], index, command[3]);
            return CommandResult.simpleString("OK");
        } catch (NumberFormatException e) {
            return CommandResult.error("ERR value is not an integer or out of range");
        } catch (IndexOutOfBoundsException e) {
            return CommandResult.error("ERR index out of range");
        } catch (IllegalArgumentException e) {
            return CommandResult.error("ERR " + e.getMessage());
        }
    }

    private CommandResult handleLrem(String[] command) {
        if (command.length != 4) {
            return CommandResult.error("ERR wrong number of arguments for 'lrem'");
        }
        try {
            int count = Integer.parseInt(command[2]);
            return CommandResult.integer(database.lrem(command[1], count, command[3]));
        } catch (NumberFormatException e) {
            return CommandResult.error("ERR value is not an integer or out of range");
        }
    }

    private CommandResult handleLinsert(String[] command) {
        if (command.length != 5) {
            return CommandResult.error("ERR wrong number of arguments for 'linsert'");
        }
        String pos = command[2].toUpperCase();
        boolean before;
        if ("BEFORE".equals(pos)) {
            before = true;
        } else if ("AFTER".equals(pos)) {
            before = false;
        } else {
            return CommandResult.error("ERR syntax error");
        }
        return CommandResult.integer(database.linsert(command[1], before, command[3], command[4]));
    }

    private CommandResult handleLtrim(String[] command) {
        if (command.length != 4) {
            return CommandResult.error("ERR wrong number of arguments for 'ltrim'");
        }
        try {
            int start = Integer.parseInt(command[2]);
            int stop  = Integer.parseInt(command[3]);
            database.ltrim(command[1], start, stop);
            return CommandResult.simpleString("OK");
        } catch (NumberFormatException e) {
            return CommandResult.error("ERR value is not an integer or out of range");
        }
    }

    private CommandResult handleBlockingPop(String[] command, boolean left) {
        String cmdName = left ? "blpop" : "brpop";
        if (command.length < 3) {
            return CommandResult.error("ERR wrong number of arguments for '" + cmdName + "'");
        }
        try {
            double timeout = Double.parseDouble(command[command.length - 1]);
            if (Double.isNaN(timeout) || Double.isInfinite(timeout) || timeout < 0) {
                return CommandResult.error("ERR timeout is negative or not a valid float");
            }
        } catch (NumberFormatException e) {
            return CommandResult.error("ERR timeout is not a float or out of range");
        }
        String[] keys = Arrays.copyOfRange(command, 1, command.length - 1);
        List<String> popped = left ? database.blpopTry(keys) : database.brpopTry(keys);
        return popped == null ? CommandResult.nil() : CommandResult.array(popped);
    }
}

