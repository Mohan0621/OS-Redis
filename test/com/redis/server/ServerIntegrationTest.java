package com.redis.server;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import com.redis.db.CommandHandler;
import com.redis.db.RedisDatabase;
import com.redis.partition.CommandRouter;
import com.redis.pool.ConnectionPool;
import com.redis.pool.ConnectionTask;

class ServerIntegrationTest {

    @TempDir
    Path tempDir;

    private static class TestServer implements AutoCloseable {
        final AofLogger aof;
        final RedisDatabase db;
        final CommandHandler handler;
        final CommandRouter router;
        final ConnectionPool pool;
        final ServerSocket serverSocket;
        final Thread acceptThread;

        TestServer(Path aofPath) throws IOException {
            this.aof = new AofLogger(aofPath.toString());
            this.db = new RedisDatabase(aof);
            this.handler = new CommandHandler(db);
            db.setRecovering(true);
            aof.replay(handler);
            db.setRecovering(false);

            this.router = new CommandRouter(4);
            this.pool = new ConnectionPool(4, 16, handler, db, router);
            this.serverSocket = new ServerSocket(0);
            this.acceptThread = new Thread(() -> {
                while (!serverSocket.isClosed()) {
                    try {
                        Socket s = serverSocket.accept();
                        pool.submit(new ConnectionTask(s), 500, TimeUnit.MILLISECONDS);
                    } catch (IOException e) {
                        if (serverSocket.isClosed()) break;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }, "test-accept");
            this.acceptThread.start();
        }

        int port() {
            return serverSocket.getLocalPort();
        }

        @Override
        public void close() throws IOException {
            serverSocket.close();
            try {
                router.shutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            try {
                pool.shutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            aof.close();
            try {
                acceptThread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    void tcpCommandsBlockingListPopAndAofReplayWorkEndToEnd() throws Exception {
        Path aofFile = tempDir.resolve("test.aof");

        try (TestServer server = new TestServer(aofFile)) {
            int port = server.port();

            try (Socket client = new Socket("localhost", port)) {
                client.setSoTimeout(5000);
                OutputStream out = client.getOutputStream();
                InputStream in = client.getInputStream();

                // SET, EXPIRE, PERSIST, ZADD duplicate scores
                assertEquals("+OK", sendAndRead(out, in, "SET", "k1", "v1"));
                assertEquals(":1", sendAndRead(out, in, "EXPIRE", "k1", "60"));
                assertEquals(":1", sendAndRead(out, in, "PERSIST", "k1"));
                assertEquals(":-1", sendAndRead(out, in, "TTL", "k1"));

                assertEquals(":1", sendAndRead(out, in, "ZADD", "scores", "10", "alice"));
                assertEquals(":1", sendAndRead(out, in, "ZADD", "scores", "10", "bob"));
                assertEquals("[alice, bob]", sendAndRead(out, in, "ZRANGE", "scores", "0", "-1"));
            }

            // BLPOP blocks until another client pushes
            CompletableFuture<String> blpopFuture = CompletableFuture.supplyAsync(() -> {
                try (Socket waiter = new Socket("localhost", port)) {
                    waiter.setSoTimeout(5000);
                    return sendAndRead(waiter.getOutputStream(), waiter.getInputStream(),
                            "BLPOP", "jobs", "3");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

            Thread.sleep(150);
            try (Socket pusher = new Socket("localhost", port)) {
                pusher.setSoTimeout(5000);
                assertEquals(":1", sendAndRead(pusher.getOutputStream(), pusher.getInputStream(),
                        "RPUSH", "jobs", "job-42"));
            }

            assertEquals("[jobs, job-42]", blpopFuture.get(3, TimeUnit.SECONDS));
        }

        // Restart server against the same AOF file and verify full state recovery
        try (TestServer restarted = new TestServer(aofFile);
             Socket client = new Socket("localhost", restarted.port())) {
            client.setSoTimeout(5000);
            OutputStream out = client.getOutputStream();
            InputStream in = client.getInputStream();

            assertEquals("v1", sendAndRead(out, in, "GET", "k1"));
            assertEquals(":-1", sendAndRead(out, in, "TTL", "k1"));
            assertEquals("[alice, bob]", sendAndRead(out, in, "ZRANGE", "scores", "0", "-1"));
            assertEquals(":0", sendAndRead(out, in, "LLEN", "jobs"));
        }
    }

    private static String sendAndRead(OutputStream out, InputStream in, String... parts)
            throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        buf.write(("*" + parts.length + "\r\n").getBytes(StandardCharsets.UTF_8));
        for (String p : parts) {
            byte[] b = p.getBytes(StandardCharsets.UTF_8);
            buf.write(("$" + b.length + "\r\n").getBytes(StandardCharsets.UTF_8));
            buf.write(b);
            buf.write("\r\n".getBytes(StandardCharsets.UTF_8));
        }
        out.write(buf.toByteArray());
        out.flush();
        return readResp(in);
    }

    private static String readResp(InputStream in) throws IOException {
        int prefix = in.read();
        if (prefix == -1) throw new IOException("EOF");
        switch (prefix) {
            case '+', '-', ':' -> {
                return (char) prefix + readLine(in);
            }
            case '$' -> {
                int len = Integer.parseInt(readLine(in));
                if (len == -1) return "(nil)";
                byte[] data = in.readNBytes(len);
                in.read();
                in.read();
                return new String(data, StandardCharsets.UTF_8);
            }
            case '*' -> {
                int count = Integer.parseInt(readLine(in));
                if (count == -1) return "(nil)";
                List<String> items = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    items.add(readResp(in));
                }
                return items.toString();
            }
            default -> throw new IOException("Unexpected prefix: " + prefix);
        }
    }

    private static String readLine(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                in.read();
                break;
            }
            sb.append((char) b);
        }
        return sb.toString();
    }
}
