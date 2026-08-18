public class CommandHandler {

    private final RedisDatabase database;

    public CommandHandler(RedisDatabase database) {
        this.database = database;
    }

    public CommandResult execute(RespCommand command) {
        return execute(command.getArguments());
    }

    public CommandResult execute(String[] command) {

        String operation = command[0].toUpperCase();

        switch (operation) {
            case "SET":
                return handleSet(command);
            case "GET":
                return handleGet(command);
            case "DEL":
                return handleDelete(command);
            case "INCR":
                return handleIncrement(command);
            case "TTL":
                return handleTtl(command);
            default:
                return CommandResult.error("ERR unknown command");
        }
    }

    private CommandResult handleSet(String[] command) {

        if (command.length < 3) {
            return CommandResult.error("ERR wrong number of arguments");
        }

        String key = command[1];
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
            return CommandResult.error("ERR wrong number of arguments");
        }

        String value = database.get(command[1]);

        return value == null ? CommandResult.nil() : CommandResult.bulkString(value);
    }

    private CommandResult handleDelete(String[] command) {

        if (command.length != 2) {
            return CommandResult.error("ERR wrong number of arguments");
        }

        String deleted = database.delete(command[1]);
        return CommandResult.integer(deleted != null ? 1 : 0);
    }

    private CommandResult handleIncrement(String[] command) {

        if (command.length != 2) {
            return CommandResult.error("ERR wrong number of arguments");
        }

        try {
            String value = database.increment(command[1]);
            return CommandResult.integer(Integer.parseInt(value));
        } catch (NumberFormatException e) {
            return CommandResult.error("ERR value is not an integer");
        }
    }

    private CommandResult handleTtl(String[] command) {

        if (command.length != 2) {
            return CommandResult.error("ERR wrong number of arguments");
        }

        long ttl = database.ttl(command[1]);
        return CommandResult.integer((int) ttl);
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
                    if (index + 1 >= command.length) throw new IllegalArgumentException("EX requires a value");
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
