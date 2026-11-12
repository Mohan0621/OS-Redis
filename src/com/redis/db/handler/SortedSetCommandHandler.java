package com.redis.db.handler;

import java.util.Arrays;
import com.redis.db.CommandResult;
import com.redis.db.RedisDatabase;

/**
 * Handles Redis Sorted Set commands.
 */
public class SortedSetCommandHandler {

    private final RedisDatabase database;

    public SortedSetCommandHandler(RedisDatabase database) {
        this.database = database;
    }

    public CommandResult handle(String operation, String[] command) {
        return switch (operation) {
            case "ZADD"             -> handleZadd(command);
            case "ZREM"             -> handleZrem(command);
            case "ZRANGE"           -> handleZrange(command, false);
            case "ZREVRANGE"        -> handleZrange(command, true);
            case "ZSCORE"           -> handleZscore(command);
            case "ZCARD"            -> handleZcard(command);
            case "ZRANK"            -> handleZrank(command, false);
            case "ZREVRANK"         -> handleZrank(command, true);
            case "ZINCRBY"          -> handleZincrby(command);
            case "ZRANGEBYSCORE"    -> handleZrangeByScore(command);
            case "ZCOUNT"           -> handleZcount(command);
            case "ZREMRANGEBYRANK"  -> handleZremRangeByRank(command);
            case "ZREMRANGEBYSCORE" -> handleZremRangeByScore(command);
            case "ZRANGEBYLEX"      -> handleZrangeByLex(command);
            case "ZLEXCOUNT"        -> handleZlexCount(command);
            default -> CommandResult.error("ERR unknown command '" + command[0] + "'");
        };
    }

    private CommandResult handleZadd(String[] command) {
        if (command.length < 4 || (command.length - 2) % 2 != 0) {
            return CommandResult.error("ERR wrong number of arguments for 'zadd'");
        }
        try {
            int pairCount = (command.length - 2) / 2;
            if (pairCount == 1) {
                double score = Double.parseDouble(command[2]);
                if (Double.isNaN(score)) {
                    return CommandResult.error("ERR value is not a valid float");
                }
                return CommandResult.integer(database.zadd(command[1], score, command[3]));
            }
            double[] scores = new double[pairCount];
            String[] members = new String[pairCount];
            for (int i = 0; i < pairCount; i++) {
                double score = Double.parseDouble(command[2 + i * 2]);
                if (Double.isNaN(score)) {
                    return CommandResult.error("ERR value is not a valid float");
                }
                scores[i] = score;
                members[i] = command[3 + i * 2];
            }
            return CommandResult.integer(database.zaddMultiple(command[1], scores, members));
        } catch (IllegalArgumentException e) {
            return CommandResult.error("ERR value is not a valid float");
        }
    }

    private CommandResult handleZrem(String[] command) {
        if (command.length < 3) {
            return CommandResult.error("ERR wrong number of arguments for 'zrem'");
        }
        String[] members = Arrays.copyOfRange(command, 2, command.length);
        return CommandResult.integer(database.zrem(command[1], members));
    }

    private CommandResult handleZrange(String[] command, boolean reverse) {
        String cmdName = reverse ? "zrevrange" : "zrange";
        if (command.length != 4 && command.length != 5) {
            return CommandResult.error("ERR wrong number of arguments for '" + cmdName + "'");
        }
        boolean withScores = false;
        if (command.length == 5) {
            if (!"WITHSCORES".equalsIgnoreCase(command[4])) {
                return CommandResult.error("ERR syntax error");
            }
            withScores = true;
        }
        try {
            int start = Integer.parseInt(command[2]);
            int stop  = Integer.parseInt(command[3]);
            return CommandResult.array(database.zrange(command[1], start, stop, withScores, reverse));
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

    private CommandResult handleZrank(String[] command, boolean reverse) {
        String cmdName = reverse ? "zrevrank" : "zrank";
        if (command.length != 3) {
            return CommandResult.error("ERR wrong number of arguments for '" + cmdName + "'");
        }
        Integer rank = reverse
                ? database.zrevrank(command[1], command[2])
                : database.zrank(command[1], command[2]);
        return rank == null ? CommandResult.nil() : CommandResult.integer(rank);
    }

    private CommandResult handleZincrby(String[] command) {
        if (command.length != 4) {
            return CommandResult.error("ERR wrong number of arguments for 'zincrby'");
        }
        try {
            double delta = Double.parseDouble(command[2]);
            double updated = database.zincrby(command[1], delta, command[3]);
            return CommandResult.bulkString(String.valueOf(updated));
        } catch (IllegalArgumentException e) {
            return CommandResult.error("ERR value is not a valid float");
        }
    }

    private CommandResult handleZrangeByScore(String[] command) {
        if (command.length != 4 && command.length != 5) {
            return CommandResult.error("ERR wrong number of arguments for 'zrangebyscore'");
        }
        boolean withScores = false;
        if (command.length == 5) {
            if (!"WITHSCORES".equalsIgnoreCase(command[4])) {
                return CommandResult.error("ERR syntax error");
            }
            withScores = true;
        }
        try {
            return CommandResult.array(
                    database.zrangeByScore(command[1], command[2], command[3], withScores));
        } catch (IllegalArgumentException e) {
            return CommandResult.error("ERR min or max is not a float");
        }
    }

    private CommandResult handleZcount(String[] command) {
        if (command.length != 4) {
            return CommandResult.error("ERR wrong number of arguments for 'zcount'");
        }
        try {
            return CommandResult.integer(database.zcount(command[1], command[2], command[3]));
        } catch (IllegalArgumentException e) {
            return CommandResult.error("ERR min or max is not a float");
        }
    }

    private CommandResult handleZremRangeByRank(String[] command) {
        if (command.length != 4) {
            return CommandResult.error("ERR wrong number of arguments for 'zremrangebyrank'");
        }
        try {
            int start = Integer.parseInt(command[2]);
            int stop  = Integer.parseInt(command[3]);
            return CommandResult.integer(database.zremRangeByRank(command[1], start, stop));
        } catch (NumberFormatException e) {
            return CommandResult.error("ERR value is not an integer or out of range");
        }
    }

    private CommandResult handleZremRangeByScore(String[] command) {
        if (command.length != 4) {
            return CommandResult.error("ERR wrong number of arguments for 'zremrangebyscore'");
        }
        try {
            return CommandResult.integer(
                    database.zremRangeByScore(command[1], command[2], command[3]));
        } catch (IllegalArgumentException e) {
            return CommandResult.error("ERR min or max is not a float");
        }
    }

    private CommandResult handleZrangeByLex(String[] command) {
        if (command.length != 4) {
            return CommandResult.error("ERR wrong number of arguments for 'zrangebylex'");
        }
        try {
            return CommandResult.array(database.zrangeByLex(command[1], command[2], command[3]));
        } catch (IllegalArgumentException e) {
            return CommandResult.error("ERR " + e.getMessage());
        }
    }

    private CommandResult handleZlexCount(String[] command) {
        if (command.length != 4) {
            return CommandResult.error("ERR wrong number of arguments for 'zlexcount'");
        }
        try {
            return CommandResult.integer(database.zlexCount(command[1], command[2], command[3]));
        } catch (IllegalArgumentException e) {
            return CommandResult.error("ERR " + e.getMessage());
        }
    }
}

