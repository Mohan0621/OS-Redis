package com.redis.stress;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

/**
 * Interactive Redis REPL client.
 * Type commands like: SET name Mohan
 * Type QUIT or EXIT to disconnect.
 */
public class InteractiveClient {

    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = 6379;

        System.out.println("Connecting to " + host + ":" + port + " ...");
        Socket socket = new Socket(host, port);
        System.out.println("Connected! Type commands (e.g. SET key value, GET key). Type EXIT to quit.\n");

        OutputStream out = socket.getOutputStream();
        InputStream in   = socket.getInputStream();
        Scanner scanner  = new Scanner(System.in);

        while (true) {
            System.out.print("> ");
            if (!scanner.hasNextLine()) break;
            String line = scanner.nextLine().trim();
            if (line.isEmpty()) continue;
            if (line.equalsIgnoreCase("QUIT") || line.equalsIgnoreCase("EXIT")) {
                System.out.println("Bye!");
                break;
            }

            // Split on whitespace, respecting quoted strings
            String[] parts = splitCommand(line);
            if (parts.length == 0) continue;

            // Build RESP array
            StringBuilder resp = new StringBuilder();
            resp.append("*").append(parts.length).append("\r\n");
            for (String part : parts) {
                byte[] bytes = part.getBytes(StandardCharsets.UTF_8);
                resp.append("$").append(bytes.length).append("\r\n");
                resp.append(part).append("\r\n");
            }

            out.write(resp.toString().getBytes(StandardCharsets.UTF_8));
            out.flush();

            // Read and print response
            String response = readResponse(in);
            System.out.println(response);
        }

        socket.close();
    }

    /** Splits on spaces; treats "quoted strings" as a single token. */
    private static String[] splitCommand(String line) {
        java.util.List<String> parts = new java.util.ArrayList<>();
        boolean inQuote = false;
        StringBuilder current = new StringBuilder();
        for (char c : line.toCharArray()) {
            if (c == '"') {
                inQuote = !inQuote;
            } else if (c == ' ' && !inQuote) {
                if (current.length() > 0) {
                    parts.add(current.toString());
                    current.setLength(0);
                }
            } else {
                current.append(c);
            }
        }
        if (current.length() > 0) parts.add(current.toString());
        return parts.toArray(new String[0]);
    }

    /** Reads one complete RESP response and returns it as a human-readable string. */
    private static String readResponse(InputStream in) throws Exception {
        // Read one byte at a time to find the first line
        StringBuilder line = new StringBuilder();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') continue;
            if (b == '\n') break;
            line.append((char) b);
        }
        String first = line.toString();
        if (first.isEmpty()) return "(empty response)";

        char type = first.charAt(0);
        String payload = first.substring(1);

        switch (type) {
            case '+': // Simple string
                return payload;
            case '-': // Error
                return "(error) " + payload;
            case ':': // Integer
                return "(integer) " + payload;
            case '$': { // Bulk string
                int len = Integer.parseInt(payload);
                if (len == -1) return "(nil)";
                byte[] buf = new byte[len + 2]; // +2 for \r\n
                int read = 0;
                while (read < buf.length) {
                    int n = in.read(buf, read, buf.length - read);
                    if (n == -1) break;
                    read += n;
                }
                return "\"" + new String(buf, 0, len, StandardCharsets.UTF_8) + "\"";
            }
            case '*': { // Array
                int count = Integer.parseInt(payload);
                if (count == -1) return "(nil)";
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < count; i++) {
                    sb.append((i + 1)).append(") ").append(readResponse(in));
                    if (i < count - 1) sb.append("\n");
                }
                return sb.toString();
            }
            default:
                return first;
        }
    }
}
