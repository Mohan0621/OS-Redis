package com.redis.stress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * RESP-aware TCP stress-test client.
 *
 * <p>Connects to localhost:6379, sends exactly 10 commands mixing all
 * Redis data types, reads and prints each response, then closes the
 * connection cleanly.  Designed to be run from multiple threads
 * simultaneously by {@link StressTest}.
 *
 * <p>Thread-safety: each instance owns its own socket and counters;
 * no shared mutable state.
 */
public class StressTestClient implements Runnable {

    private static final String HOST    = "localhost";
    private static final int    PORT    = 6379;
    private static final int    CONNECT_TIMEOUT_MS = 5_000;
    private static final int    READ_TIMEOUT_MS    = 10_000;

    private final int clientId;
    private int successCount = 0;
    private int errorCount   = 0;

    // ----------------------------------------------------------------
    // Construction
    // ----------------------------------------------------------------

    /**
     * @param clientId unique identifier used to generate distinct keys
     */
    public StressTestClient(int clientId) {
        this.clientId = clientId;
    }

    // ----------------------------------------------------------------
    // Runnable
    // ----------------------------------------------------------------

    @Override
    public void run() {

        Socket socket = new Socket();

        try {

            socket.connect(
                    new InetSocketAddress(HOST, PORT),
                    CONNECT_TIMEOUT_MS);
            socket.setSoTimeout(READ_TIMEOUT_MS);

            InputStream  in  = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            for (int i = 0; i < 10; i++) {

                byte[] cmd = buildCommand(i);

                out.write(cmd);
                out.flush();

                String response = readRespResponse(in);

                System.out.println(
                        "[client-" + clientId + "] cmd" + i + ": "
                        + response.trim());

                // A RESP error line starts with '-'
                if (response.startsWith("-")) {
                    errorCount++;
                } else {
                    successCount++;
                }
            }

        } catch (IOException e) {
            System.out.println(
                    "[client-" + clientId + "] ERROR: " + e.getMessage());
        } finally {
            try {
                socket.close();
            } catch (IOException ignored) {}
        }
    }

    // ----------------------------------------------------------------
    // Accessors
    // ----------------------------------------------------------------

    public int getSuccessCount() {
        return successCount;
    }

    public int getErrorCount() {
        return errorCount;
    }

    // ----------------------------------------------------------------
    // Private helpers
    // ----------------------------------------------------------------

    /**
     * Build the RESP command for loop index {@code i}.
     */
    private byte[] buildCommand(int i) {

        String c = String.valueOf(clientId);

        switch (i) {
            case 0:
                return buildRespCommand(
                        "SET",
                        "client:" + c + ":key:" + i,
                        "value" + i);
            case 1:
                return buildRespCommand(
                        "GET",
                        "client:" + c + ":key:0");
            case 2:
                return buildRespCommand(
                        "INCR",
                        "client:" + c + ":counter");
            case 3:
                return buildRespCommand(
                        "LPUSH",
                        "client:" + c + ":list",
                        "item" + i);
            case 4:
                return buildRespCommand(
                        "HSET",
                        "client:" + c + ":hash",
                        "field" + i,
                        "val" + i);
            case 5:
                return buildRespCommand(
                        "SADD",
                        "client:" + c + ":set",
                        "member" + i);
            case 6:
                return buildRespCommand(
                        "ZADD",
                        "client:" + c + ":zset",
                        i + ".0",
                        "member" + i);
            case 7:
                return buildRespCommand(
                        "SET",
                        "client:" + c + ":key:" + i,
                        "value" + i);
            case 8:
                return buildRespCommand(
                        "GET",
                        "client:" + c + ":key:" + i);
            case 9:
                return buildRespCommand(
                        "INCR",
                        "client:" + c + ":counter");
            default:
                return buildRespCommand("PING");
        }
    }

    /**
     * Encode a RESP array from the given parts.
     *
     * <pre>
     *   *N\r\n
     *   $len\r\n
     *   bulk\r\n
     *   ...
     * </pre>
     *
     * @param parts command tokens (command name first, then arguments)
     * @return encoded RESP byte array
     */
    private byte[] buildRespCommand(String... parts) {

        StringBuilder sb = new StringBuilder();

        sb.append('*').append(parts.length).append("\r\n");

        for (String part : parts) {
            byte[] bytes = part.getBytes(StandardCharsets.UTF_8);
            sb.append('$').append(bytes.length).append("\r\n");
            sb.append(part).append("\r\n");
        }

        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Read one complete RESP value from {@code in}.
     *
     * <ul>
     *   <li>{@code +} simple string — returns the full line including prefix</li>
     *   <li>{@code -} error        — returns the full line including prefix</li>
     *   <li>{@code :} integer      — returns the full line including prefix</li>
     *   <li>{@code $} bulk string  — returns the string content</li>
     *   <li>{@code *} array        — returns elements joined by space</li>
     * </ul>
     *
     * @param in the socket's input stream
     * @return string representation of the response
     * @throws IOException on read error or unexpected EOF
     */
    private String readRespResponse(InputStream in) throws IOException {

        int type = in.read();

        if (type == -1) {
            throw new IOException("Connection closed by server");
        }

        switch (type) {

            case '+': // simple string
            case '-': // error
            case ':': { // integer
                String line = readLine(in);
                return (char) type + line;
            }

            case '$': { // bulk string
                String lenLine = readLine(in);
                int length = Integer.parseInt(lenLine.trim());

                if (length == -1) {
                    return "(null)";
                }

                byte[] data = new byte[length];
                int    read = 0;

                while (read < length) {
                    int n = in.read(data, read, length - read);
                    if (n == -1) {
                        throw new IOException("Unexpected EOF reading bulk string");
                    }
                    read += n;
                }

                // consume trailing \r\n
                in.read(); // \r
                in.read(); // \n

                return new String(data, StandardCharsets.UTF_8);
            }

            case '*': { // array
                String countLine = readLine(in);
                int count = Integer.parseInt(countLine.trim());

                if (count == -1) {
                    return "(null)";
                }

                StringBuilder result = new StringBuilder();

                for (int i = 0; i < count; i++) {
                    if (i > 0) {
                        result.append(' ');
                    }
                    result.append(readRespResponse(in));
                }

                return result.toString();
            }

            default:
                throw new IOException(
                        "Unknown RESP type byte: " + type);
        }
    }

    /**
     * Read bytes from {@code in} until {@code \r\n}, returning the
     * content before the terminator (not including {@code \r\n}).
     */
    private String readLine(InputStream in) throws IOException {

        StringBuilder sb = new StringBuilder();
        int b;

        while ((b = in.read()) != -1) {
            if (b == '\r') {
                in.read(); // consume \n
                break;
            }
            sb.append((char) b);
        }

        return sb.toString();
    }
}
