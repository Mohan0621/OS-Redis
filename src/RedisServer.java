import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Redis server entry point — v2.
 *
 * Architecture wired here
 * -----------------------
 *
 *   TCP accept loop
 *       │
 *       ▼
 *   RedisWorkerPool     ← connection-handler pool (thin I/O threads)
 *       │  (4 workers, queue=8)
 *       ▼
 *   RedisClientHandler  ← parses RESP, routes each command
 *       │
 *       ▼
 *   CommandRouter       ← classifies verb → DataTypePartition
 *       │
 *       ├─ STRING (4 partitions, 2 workers each = 4 workers)
 *       ├─ LIST   (2 partitions)
 *       ├─ HASH   (2 partitions)
 *       ├─ SET    (2 partitions)
 *       └─ ZSET   (2 partitions)
 *             │
 *         shared Semaphore(8)  ← caps total concurrent DB ops
 *             │
 *         CommandHandler → RedisDatabase
 *
 * Two pools, two jobs
 * -------------------
 * connectionPool   — one worker per active client connection.
 *                    Spends most of its time blocked on socket read.
 *                    4 workers is fine because I/O is the bottleneck here.
 *
 * CommandRouter    — one worker per partition lane.
 *                    These are the CPU/memory workers that actually touch
 *                    data structures.  Gated by the shared semaphore.
 *
 * Shutdown sequence (Ctrl+C via JVM shutdown hook)
 * -------------------------------------------------
 *   1. Stop accepting new connections (close ServerSocket).
 *   2. Shut down CommandRouter — drains all partition queues.
 *   3. Shut down connectionPool — in-flight handlers complete.
 *   4. Stop expiration scheduler.
 */
public class RedisServer {

    private static final int PORT             = 6379;
    private static final int CONN_WORKERS     = 4;
    private static final int CONN_QUEUE_CAP   = 8;
    private static final int MONITOR_INTERVAL = 5_000;   // ms

    public static void main(String[] args) throws IOException {

        // ---- Database layer ----
        AofLogger      aofLogger      = new AofLogger("redis.aof");
        RedisDatabase  database       = new RedisDatabase(aofLogger);
        CommandHandler commandHandler = new CommandHandler(database);

        // AOF replay on startup
        database.setRecovering(true);
        aofLogger.replay(commandHandler);
        database.setRecovering(false);

        // ---- Command routing layer ----
        CommandRouter router = new CommandRouter(8);   // 8 shared DB permits

        // ---- Connection handling pool ----
        // Thin pool: workers spend most time waiting on socket reads.
        // We do NOT route commands from these workers — they just parse
        // and hand off to CommandRouter immediately.
        RedisWorkerPool connectionPool =
                new RedisWorkerPool(CONN_WORKERS, CONN_QUEUE_CAP);

        // ---- Background expiration sweep ----
        ScheduledExecutorService expirationScheduler =
                Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "expiration-sweeper");
                    t.setDaemon(true);
                    return t;
                });

        expirationScheduler.scheduleAtFixedRate(
                database::removeExpiredKeys, 1, 1, TimeUnit.SECONDS);

        // ---- Pool health monitor ----
        Thread monitorThread = new Thread(
                new RedisPoolMonitor(connectionPool, MONITOR_INTERVAL),
                "pool-monitor"
        );
        monitorThread.setDaemon(true);
        monitorThread.start();

        // ---- Shutdown hook ----
        ServerSocket serverSocket = new ServerSocket(PORT);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutdown signal received — draining...");
            try {
                serverSocket.close();
            } catch (IOException ignored) {}
            try {
                router.shutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            try {
                connectionPool.shutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            expirationScheduler.shutdown();
            System.out.println("Server stopped cleanly.");
        }, "shutdown-hook"));

        System.out.println("Redis server started on port " + PORT);
        System.out.printf(
                "Connection pool: %d workers, queue=%d%n",
                CONN_WORKERS, CONN_QUEUE_CAP);
        System.out.printf(
                "Command router:  %d total partition workers, 8 DB permits%n",
                router.totalWorkers());
        System.out.println();

        // ---- Accept loop ----
        while (!serverSocket.isClosed()) {

            Socket clientSocket;
            try {
                clientSocket = serverSocket.accept();
            } catch (IOException e) {
                if (serverSocket.isClosed()) break;
                System.err.println("Accept error: " + e.getMessage());
                continue;
            }

            System.out.println("Client connected: "
                    + clientSocket.getRemoteSocketAddress());

            try {

                connectionPool.submit(
                        new RedisClientHandler(
                                clientSocket,
                                commandHandler,
                                database,
                                router
                        ),
                        100,
                        TimeUnit.MILLISECONDS
                );

            } catch (TaskRejectedException e) {

                System.err.println("Connection rejected (pool full): "
                        + clientSocket.getRemoteSocketAddress());
                try {
                    clientSocket.getOutputStream()
                            .write("-ERR server busy\r\n".getBytes());
                    clientSocket.close();
                } catch (IOException ignored) {}

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
