package com.redis.protocol;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import com.redis.db.CommandResult;

public class RespWriter {

    private final OutputStream output;

    public RespWriter(OutputStream output) {
        this.output = output;
    }

    public void writeSimpleString(String value) throws IOException {
        writeRaw("+" + value + "\r\n");
    }

    public void writeBulkString(String value) throws IOException {
        if (value == null) {
            writeRaw("$-1\r\n");
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        writeRaw("$" + bytes.length + "\r\n");
        output.write(bytes);
        writeRaw("\r\n");
    }

    public void writeInteger(int value) throws IOException {
        writeRaw(":" + value + "\r\n");
    }

    public void writeError(String message) throws IOException {
        writeRaw("-" + message + "\r\n");
    }

    public void write(CommandResult result) throws IOException {

        switch (result.getType()) {

            case SIMPLE_STRING:
                writeSimpleString(result.getValue());
                break;

            case BULK_STRING:
                writeBulkString(result.getValue());
                break;

            case INTEGER:
                writeInteger(Integer.parseInt(result.getValue()));
                break;

            case ERROR:
                writeError(result.getValue());
                break;

            case NULL:
                writeRaw("$-1\r\n");
                break;

            case ARRAY:
                writeArray(result.getElements());
                break;
        }
    }

    public void writeArray(java.util.List<String> elements) throws IOException {
        writeRaw("*" + elements.size() + "\r\n");
        for (String element : elements) {
            writeBulkString(element);   // null element → $-1\r\n
        }
    }

    private void writeRaw(String value) throws IOException {
        output.write(value.getBytes(StandardCharsets.UTF_8));
        output.flush();
    }
}
