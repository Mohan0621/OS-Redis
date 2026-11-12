package com.redis.db.handler;

import java.util.ArrayList;
import java.util.Arrays;
import com.redis.db.CommandResult;
import com.redis.db.RedisDatabase;

/**
 * Handles Redis Set commands.
 */
public class SetCommandHandler {

    private final RedisDatabase database;

    public SetCommandHandler(RedisDatabase database) {
        this.database = database;
    }

    public CommandResult handle(String operation, String[] command) {
        return switch (operation) {
            case "SADD"        -> handleSadd(command);
            case "SREM"        -> handleSrem(command);
            case "SISMEMBER"   -> handleSismember(command);
            case "SCARD"       -> handleScard(command);
            case "SMEMBERS"    -> handleSmembers(command);
            case "SUNION"      -> handleSunion(command);
            case "SINTER"      -> handleSinter(command);
            case "SDIFF"       -> handleSdiff(command);
            case "SUNIONSTORE" -> handleSunionstore(command);
            case "SINTERSTORE" -> handleSinterstore(command);
            case "SDIFFSTORE"  -> handleSdiffstore(command);
            case "SMOVE"       -> handleSmove(command);
            case "SRANDMEMBER" -> handleSrandmember(command);
            case "SPOP"        -> handleSpop(command);
            default -> CommandResult.error("ERR unknown command '" + command[0] + "'");
        };
    }

    private CommandResult handleSadd(String[] command) {
        if (command.length < 3) {
            return CommandResult.error("ERR wrong number of arguments for 'sadd'");
        }
        String[] members = Arrays.copyOfRange(command, 2, command.length);
        return CommandResult.integer(database.sadd(command[1], members));
    }

    private CommandResult handleSrem(String[] command) {
        if (command.length < 3) {
            return CommandResult.error("ERR wrong number of arguments for 'srem'");
        }
        String[] members = Arrays.copyOfRange(command, 2, command.length);
        return CommandResult.integer(database.srem(command[1], members));
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
        return CommandResult.array(new ArrayList<>(database.smembers(command[1])));
    }

    private CommandResult handleSunion(String[] command) {
        if (command.length < 2) {
            return CommandResult.error("ERR wrong number of arguments for 'sunion'");
        }
        String[] keys = Arrays.copyOfRange(command, 1, command.length);
        return CommandResult.array(new ArrayList<>(database.sunion(keys)));
    }

    private CommandResult handleSinter(String[] command) {
        if (command.length < 2) {
            return CommandResult.error("ERR wrong number of arguments for 'sinter'");
        }
        String[] keys = Arrays.copyOfRange(command, 1, command.length);
        return CommandResult.array(new ArrayList<>(database.sinter(keys)));
    }

    private CommandResult handleSdiff(String[] command) {
        if (command.length < 2) {
            return CommandResult.error("ERR wrong number of arguments for 'sdiff'");
        }
        String[] keys = Arrays.copyOfRange(command, 1, command.length);
        return CommandResult.array(new ArrayList<>(database.sdiff(keys)));
    }

    private CommandResult handleSunionstore(String[] command) {
        if (command.length < 3) {
            return CommandResult.error("ERR wrong number of arguments for 'sunionstore'");
        }
        String[] keys = Arrays.copyOfRange(command, 2, command.length);
        return CommandResult.integer(database.sunionstore(command[1], keys));
    }

    private CommandResult handleSinterstore(String[] command) {
        if (command.length < 3) {
            return CommandResult.error("ERR wrong number of arguments for 'sinterstore'");
        }
        String[] keys = Arrays.copyOfRange(command, 2, command.length);
        return CommandResult.integer(database.sinterstore(command[1], keys));
    }

    private CommandResult handleSdiffstore(String[] command) {
        if (command.length < 3) {
            return CommandResult.error("ERR wrong number of arguments for 'sdiffstore'");
        }
        String[] keys = Arrays.copyOfRange(command, 2, command.length);
        return CommandResult.integer(database.sdiffstore(command[1], keys));
    }

    private CommandResult handleSmove(String[] command) {
        if (command.length != 4) {
            return CommandResult.error("ERR wrong number of arguments for 'smove'");
        }
        return CommandResult.integer(database.smove(command[1], command[2], command[3]));
    }

    private CommandResult handleSrandmember(String[] command) {
        if (command.length == 2) {
            String member = database.srandmember(command[1]);
            return member == null ? CommandResult.nil() : CommandResult.bulkString(member);
        }
        if (command.length == 3) {
            try {
                int count = Integer.parseInt(command[2]);
                return CommandResult.array(database.srandmember(command[1], count));
            } catch (NumberFormatException e) {
                return CommandResult.error("ERR value is not an integer or out of range");
            }
        }
        return CommandResult.error("ERR wrong number of arguments for 'srandmember'");
    }

    private CommandResult handleSpop(String[] command) {
        if (command.length == 2) {
            String member = database.spop(command[1]);
            return member == null ? CommandResult.nil() : CommandResult.bulkString(member);
        }
        if (command.length == 3) {
            try {
                int count = Integer.parseInt(command[2]);
                if (count < 0) {
                    return CommandResult.error("ERR value is out of range, must be positive");
                }
                return CommandResult.array(database.spop(command[1], count));
            } catch (NumberFormatException e) {
                return CommandResult.error("ERR value is not an integer or out of range");
            }
        }
        return CommandResult.error("ERR wrong number of arguments for 'spop'");
    }
}

