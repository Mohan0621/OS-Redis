public class CommandHandler {
    private final RedisDatabase database;

    public CommandHandler(RedisDatabase database) {
        this.database = database;
    }

    public String execute(String[] command) {
        String operation = command[0].toUpperCase();
        switch (operation) {
            case "SET":
                return handleSet(command);
            case "GET":
                return handleGet(command);
            case "DEL":
                return handleDelete(command);
            default:
                return "ERR unknown command";
        }
    }

    private String handleSet(String[] command) {
        if (command.length != 3) {
            return "ERR wrong number of arguments";
        }
        String key = command[1];
        String value = command[2];
        database.set(key, value);
        return "OK";
    }

    private String handleGet(String[] command) {
        if (command.length != 2) {
            return "ERR wrong number of arguments";
        }
        String key = command[1];
        String value = database.get(key);
        if (value == null) {
            return "(nil)";
        }
        return value;
    }

    private String handleDelete(String[] command) {
        if (command.length != 2) {
            return "ERR wrong number of arguments";
        }
        String key = command[1];
        String deleted = database.delete(key);
        return deleted != null ? "1" : "0";
    }
}
