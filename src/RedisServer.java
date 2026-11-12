import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Redis server entry point — v3.
 *
 * Architecture
 * ------------
 *
 *   TCP accept loop
 *       │
 *       ▼
 *   ConnectionPool          ← custom pool; workers own the full client session
 *       │  (4 workers, queue=8)
 *       │
 *       │  Each worker builds a RedisClientHandler and calls run().
 *       │  The handler reads RESP commands from the socket, wraps each
 *       │  command in a Runnable closure, and submits it to CommandRouter.
 *       │  The worker does NOT hold any database permit here.
 *       │
 *       ▼
 *   RedisClientHandler
 *       │
 *       ▼
 *   CommandRouter           ← classifies verb → DataTypePartition
 *       │
 *       ├─ STRING (4 partitions)
 *       ├─ LIST   (2 partitions)
 *       ├─ HASH   (2 partitions)
 *       ├─ SET    (2 partitions)
 *       └─ ZSET   (2 partitions)
 *             │
 *         shared Semaphore(8)   ← caps total concurrent DB ops
 *             │
 *         CommandHandler → RedisDatabase
 *
 * Two independently bounded layers
 * ---------------------------------
 *   Connection layer  : ConnectionPool queue (8 slots) + 4 workers
 *   Database layer    : per-partition queues (16 slots each) + 12 workers
 *                       gated by Semaphore(8)
 *
 * Connection workers are NOT database workers.  A connection worker is
 * blocked for the duration of one client session (socket reads + waiting
 * for partition results), but it never holds a DB semaphore permit.
 * The permit is only held inside DataTypePartition.workerLoop() while
 * the actual store operation runs.
 *
 * Shutdown sequence (Ctrl+C via JVM shutdown hook)
 * -------------------------------------------------
 *   1. Stop accepting new connections (close ServerSocket).
 *   2. Shut down CommandRouter   — drains all partition queues first.
 *   3. Shut down ConnectionPool  — in-flight handlers complete.
 *   4. Stop expiration scheduler.
 */
public class RedisServer {

    private static final int PORT             = 6379;
    private static final int CONN_WORKERS     = 4;
    private static final int CONN_QUEUE_CAP   = 8;
    private static final int DB_PERMITS       = 8;
    private static final int MONITOR_INTERVAL = 5_000;   // ms
    private static final long OFFER_TIMEOUT_MS = 100;

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
        // 12 partition workers total, all sharing one Semaphore(8)
        CommandRouter router = new CommandRouter(DB_PERMITS);

        // ---- Connection pool ----
        // Workers here are I/O-bound: they spend most time on socket reads
        // and waiting for partition results. They never hold a DB permit.
        ConnectionPool connectionPool = new ConnectionPool(
                CONN_WORKERS,
                CONN_QUEUE_CAP,
                commandHandler,
                database,
                router
        );

        // ---- Background expiration sweep ----
        ScheduledExecutorService expirationScheduler =
                Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "expiration-sweeper");
                    t.setDaemon(true);
                    return t;
                });

        expirationScheduler.scheduleAtFixedRate(
                database::removeExpiredKeys, 1, 1, TimeUnit.SECONDS);

        // ---- Connection pool monitor ----
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

            // 1. Stop accepting new connections
            try {
                serverSocket.close();
            } catch (IOException ignored) {}

            // 2. Drain all partition queues (DB layer)
            try {
                router.shutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // 3. Drain in-flight connection handlers
            try {
                connectionPool.shutdown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // 4. Stop expiry sweeper
            expirationScheduler.shutdown();

            System.out.println("Server stopped cleanly.");

        }, "shutdown-hook"));

        // ---- Startup banner ----
        System.out.println("Redis server started on port " + PORT);
        System.out.printf(
                "Connection pool : %d workers, queue capacity=%d%n",
                CONN_WORKERS, CONN_QUEUE_CAP);
        System.out.printf(
                "Command router  : %d total partition workers, %d DB permits%n",
                router.totalWorkers(), DB_PERMITS);
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
                        new ConnectionTask(clientSocket),
                        OFFER_TIMEOUT_MS,
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
