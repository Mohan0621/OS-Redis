import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.io.ByteArrayOutputStream;

public class RespParser {

    private final InputStream input;

    public RespParser(InputStream input) {
        this.input = input;
    }

    public RespCommand readCommand() throws IOException {

        ByteArrayOutputStream rawBuffer = new ByteArrayOutputStream();

        int firstByte = input.read();

        if (firstByte == -1) {
            return null;
        }

        rawBuffer.write(firstByte);

        if (firstByte != '*') {
            throw new IOException("Expected RESP array");
        }

        int count = readInteger(rawBuffer);
        String[] arguments = new String[count];

        for (int i = 0; i < count; i++) {
            arguments[i] = readBulkString(rawBuffer);
        }

        return new RespCommand(arguments, rawBuffer.toByteArray());
    }

    private int readInteger(ByteArrayOutputStream rawBuffer) throws IOException {
        String line = readLine(rawBuffer);
        return Integer.parseInt(line);
    }

    private String readBulkString(ByteArrayOutputStream rawBuffer) throws IOException {
        int firstByte = input.read();
        rawBuffer.write(firstByte);

        if (firstByte != '$') {
            throw new IOException("Expected bulk string");
        }

        int length = readInteger(rawBuffer);
        byte[] data = input.readNBytes(length);
        rawBuffer.write(data);

        // consume \r\n
        int cr = input.read();
        int lf = input.read();
        rawBuffer.write(cr);
        rawBuffer.write(lf);

        return new String(data, StandardCharsets.UTF_8);
    }

    private String readLine(ByteArrayOutputStream rawBuffer) throws IOException {

        StringBuilder builder = new StringBuilder();
        int current;

        while ((current = input.read()) != -1) {

            rawBuffer.write(current);

            if (current == '\r') {
                int next = input.read();
                rawBuffer.write(next);
                if (next == '\n') {
                    break;
                }
            }

            builder.append((char) current);
        }

        return builder.toString();
    }
}
