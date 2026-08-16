import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class RespWriter {
    private final OutputStream output;

    public RespWriter(OutputStream output) {
        this.output = output;
    }

    public void writeSimpleString(String value) throws IOException {
        write("+" + value + "\r\n");
    }

    public void writeBulkString(String value) throws IOException {
        if (value == null) {
            write("$-1\r\n");
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        write("$" + bytes.length + "\r\n");
        output.write(bytes);
        write("\r\n");
    }

    public void writeInteger(int value) throws IOException {
        write(":" + value + "\r\n");
    }

    public void writeError(String message) throws IOException {
        write("-" + message + "\r\n");
    }

    private void write(String value) throws IOException {
        output.write(value.getBytes(StandardCharsets.UTF_8));
        output.flush();
    }
}
