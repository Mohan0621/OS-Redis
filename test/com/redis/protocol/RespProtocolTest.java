package com.redis.protocol;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import com.redis.db.CommandResult;

class RespProtocolTest {

    @Test
    void parsesRespArrayCommandAndPreservesRawBytes() throws IOException {
        String raw = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        RespParser parser = new RespParser(new ByteArrayInputStream(raw.getBytes(StandardCharsets.UTF_8)));

        RespCommand cmd = parser.readCommand();
        assertNotNull(cmd);
        assertArrayEquals(new String[] {"SET", "key", "value"}, cmd.getArguments());
        assertEquals(raw, new String(cmd.getRawBytes(), StandardCharsets.UTF_8));
        assertNull(parser.readCommand());
    }

    @Test
    void parsesMultipleCommandsAndUtf8Payloads() throws IOException {
        String utf8Val = "café-🚀";
        byte[] valBytes = utf8Val.getBytes(StandardCharsets.UTF_8);
        String raw = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$" + valBytes.length + "\r\n" + utf8Val + "\r\n"
                   + "*1\r\n$4\r\nPING\r\n";
        RespParser parser = new RespParser(new ByteArrayInputStream(raw.getBytes(StandardCharsets.UTF_8)));

        RespCommand first = parser.readCommand();
        assertNotNull(first);
        assertArrayEquals(new String[] {"SET", "k", utf8Val}, first.getArguments());

        RespCommand second = parser.readCommand();
        assertNotNull(second);
        assertArrayEquals(new String[] {"PING"}, second.getArguments());
        assertNull(parser.readCommand());
    }

    @Test
    void rejectsNonArrayTopLevelInput() {
        RespParser parser = new RespParser(
                new ByteArrayInputStream("+PING\r\n".getBytes(StandardCharsets.UTF_8)));
        assertThrows(IOException.class, parser::readCommand);
    }

    @Test
    void writesAllRespResultTypesIncludingLongIntegersAndNilArrayElements() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        RespWriter writer = new RespWriter(out);

        writer.write(CommandResult.simpleString("OK"));
        writer.write(CommandResult.bulkString("hello"));
        writer.write(CommandResult.integer(5_000_000_000L));
        writer.write(CommandResult.error("ERR boom"));
        writer.write(CommandResult.nil());
        writer.write(CommandResult.array(Arrays.asList("a", null, "b")));
        writer.write(CommandResult.array(Collections.emptyList()));

        String expected =
                "+OK\r\n"
                + "$5\r\nhello\r\n"
                + ":5000000000\r\n"
                + "-ERR boom\r\n"
                + "$-1\r\n"
                + "*3\r\n$1\r\na\r\n$-1\r\n$1\r\nb\r\n"
                + "*0\r\n";

        assertEquals(expected, out.toString(StandardCharsets.UTF_8));
    }
}

