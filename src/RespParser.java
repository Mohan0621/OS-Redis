import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class RespParser {

    private final InputStream input;

    public RespParser(InputStream input) {
        this.input = input;
    }

    public String[] readCommand() throws IOException {
        int firstByte = input.read();
        if (firstByte == -1) {
            return null;
        }
        if (firstByte != '*') {
            throw new IOException("Expected RESP array");
        }
        int count = readInteger();
        String[] command = new String[count];
        for (int i = 0; i < count; i++) {
            command[i] = readBulkString();
        }
        return command;
    }
    private int readInteger() throws IOException {
        String line = readLine();
        return Integer.parseInt(line);
    }
    private String readBulkString() throws IOException {
        int firstByte = input.read();
        if (firstByte != '$') {
            throw new IOException("Expected bulk string");
        }
        int length = readInteger();
        byte[] data = input.readNBytes(length);
        input.read();
        input.read();
        return new String(data, StandardCharsets.UTF_8);
    }
    private String readLine() throws IOException {

        StringBuilder builder = new StringBuilder();

        int current;

        while ((current = input.read()) != -1) {

            if (current == '\r') {
                int next = input.read();

                if (next == '\n') {
                    break;
                }
            }

            builder.append((char) current);
        }

        return builder.toString();
    }
}