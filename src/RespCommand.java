public class RespCommand {

    private final String[] arguments;
    private final byte[] rawBytes;

    public RespCommand(String[] arguments, byte[] rawBytes) {
        this.arguments = arguments;
        this.rawBytes = rawBytes;
    }

    public String[] getArguments() {
        return arguments;
    }

    public byte[] getRawBytes() {
        return rawBytes;
    }
}