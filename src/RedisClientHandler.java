import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

/**
 * Handles one TCP client connection.
 *
 * v2 changes
 * ----------
 * - Commands are no longer executed inline on the acceptor thread.
 *   Instead each command is wrapped in a Runnable and submitted to
 *   CommandRouter, which places it on the correct DataTypePartition
 *   queue based on the command verb and key.
 *
 * - The response must be written back from inside the task because
 *   the RespWriter is tied to this socket's output stream, and we
 *   must not write to it from multiple threads simultaneously.
 *   The task captures writer + result by closure — safe because
 *   each task runs on exactly one worker thread.
 *
 * - If the router rejects the task (queue full / shutting down), we
 *   immediately send a RESP error back to the client.  This is the
 *   client-visible backpressure signal.
 *
 * - AOF persistence happens inside the task, after commandHandler.execute(),
 *   so it is also serialised per-partition (no concurrent writes to AOF
 *   from the same key's operations).
 *
 * Offer timeout: 100 ms.  Enough for transient bursts; short enough
 * that a saturated server surfaces the problem quickly.
 */
public class RedisClientHandler implements Runnable {

    private static final long   OFFER_TIMEOUT_MS = 100;
    private static final String ERR_OVERLOADED   =
            "-ERR server overloaded, try again\r\n";
    private static final String ERR_SHUTDOWN     =
            "-ERR server shutting down\r\n";

    private final Socket         clientSocket;
    private final CommandHandler commandHandler;
    private final RedisDatabase  database;
    private final CommandRouter  router;

    public RedisClientHandler(
            Socket         clientSocket,
            CommandHandler commandHandler,
            RedisDatabase  database,
            CommandRouter  router) {

        this.clientSocket   = clientSocket;
        this.commandHandler = commandHandler;
        this.database       = database;
        this.router         = router;
    }

    @Override
    public void run() {

        try {

            RespParser parser =
                    new RespParser(clientSocket.getInputStream());

            RespWriter writer =
                    new RespWriter(clientSocket.getOutputStream());

            while (true) {

                RespCommand command = parser.readCommand();

                if (command == null) {
                    break;         // client disconnected
                }

                String[] args = command.getArguments();
                String   verb = args[0].toUpperCase();

                // Key is the first argument after the verb for all standard
                // Redis commands.  For keyless commands (e.g. PING, INFO)
                // we use a synthetic key so routing still works.
                String key = (args.length > 1) ? args[1] : "__keyless__";

                try {

                    router.route(
                            verb,
                            key,
                            () -> {
                                // ---- This runs on a partition worker thread ----

                                CommandResult result =
                                        commandHandler.execute(command);

                                // AOF: persist write commands only
                                if (isWriteCommand(verb)) {
                                    try {
                                        database.persist(command);
                                    } catch (Exception e) {
                                        System.err.println(
                                                "AOF write failed: " + e.getMessage());
                                    }
                                }

                                // Write response back through this socket.
                                // Synchronised on writer to guard against the
                                // rare case where pipelining could cause two
                                // tasks for the same client to race here.
                                synchronized (writer) {
                                    try {
                                        writer.write(result);
                                    } catch (IOException e) {
                                        System.err.println(
                                                "Write to client failed: "
                                                + e.getMessage());
                                    }
                                }
                            },
                            OFFER_TIMEOUT_MS,
                            TimeUnit.MILLISECONDS
                    );

                } catch (TaskRejectedException e) {

                    // The partition queue was full — tell the client immediately.
                    String errMsg = e.getMessage().contains("shutting down")
                            ? ERR_SHUTDOWN
                            : ERR_OVERLOADED;
                    try {
                        clientSocket.getOutputStream()
                                .write(errMsg.getBytes());
                    } catch (IOException ignored) {}

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

        } catch (IOException e) {

            System.out.println("Client connection error: " + e.getMessage());

        } finally {

            try {
                clientSocket.close();
            } catch (IOException ignored) {}
        }
    }

    // ----------------------------------------------------------------
    // Helper
    // ----------------------------------------------------------------

    private static boolean isWriteCommand(String verb) {
        return switch (verb) {
            case "SET", "DEL", "INCR", "DECR",
                 "MSET", "SETNX", "GETSET", "GETDEL",
                 "LPUSH", "RPUSH", "LPOP", "RPOP",
                 "LSET", "LREM", "LINSERT", "LTRIM",
                 "HSET", "HDEL", "HMSET", "HINCRBY",
                 "SADD", "SREM", "SMOVE", "SPOP",
                 "SUNIONSTORE", "SINTERSTORE", "SDIFFSTORE",
                 "ZADD", "ZREM", "ZINCRBY",
                 "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE",
                 "EXPIRE", "PEXPIRE", "PERSIST"
                    -> true;
            default -> false;
        };
    }
}
