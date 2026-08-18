public class CommandResult {

    private final RespType type;
    private final String value;

    private CommandResult(RespType type, String value) {
        this.type = type;
        this.value = value;
    }

    public static CommandResult simpleString(String value) {
        return new CommandResult(RespType.SIMPLE_STRING, value);
    }

    public static CommandResult bulkString(String value) {
        return new CommandResult(RespType.BULK_STRING, value);
    }

    public static CommandResult integer(int value) {
        return new CommandResult(RespType.INTEGER, String.valueOf(value));
    }

    public static CommandResult error(String message) {
        return new CommandResult(RespType.ERROR, message);
    }

    public static CommandResult nil() {
        return new CommandResult(RespType.NULL, null);
    }

    public RespType getType() {
        return type;
    }

    public String getValue() {
        return value;
    }
}
