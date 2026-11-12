import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Handles one TCP client connection end-to-end.
 *
 * Pipeline
 * --------
 *
 *   connection-worker thread
 *       │
 *       ▼
 *   RespParser.readCommand()
 *       │
 *       ▼
 *   CompletableFuture<CommandResult> future = new CompletableFuture<>()
 *       │
 *       ▼
 *   router.route(verb, key, () -> {
 *       result = commandHandler.execute(command)   ← runs on partition worker
 *       if write: database.persist(command)        ← AOF, still on partition worker
 *       future.complete(result)                    ← unblocks connection worker
 *   })
 *       │
 *       ▼
 *   future.join()          ← connection worker blocks here (NOT on DB semaphore)
 *       │
 *       ▼
 *   writer.write(result)   ← back on connection worker — no concurrent writers possible
 *       │
 *       ▼
 *   loop back to readCommand()
 *
 * Why future.join() instead of writing from inside the task
 * ----------------------------------------------------------
 * Writing the response from inside the partition worker (as before) means the
 * response reaches the client on whichever thread finished the DB op — which
 * is fine for a single command but breaks pipelining.  Consider:
 *
 *   Client sends:  SET x 1  →  GET x
 *
 *   SET x 1 lands on string-partition-2
 *   GET x   lands on string-partition-2   (same key → same partition)
 *
 *   Because the same partition has exactly one worker, SET always runs before GET.
 *   But if they had landed on DIFFERENT partitions (different keys, same client),
 *   the partition workers would race to call writer.write() — and even though
 *   synchronized(writer) prevents byte-interleaving, it cannot fix the ordering:
 *   whichever partition finishes first wins the lock and sends its response first,
 *   regardless of which command came first from the client.
 *
 * With future.join() the connection worker is the *only* thread that ever calls
 * writer.write(), in exactly the order commands arrived.  No lock needed.
 *
 * Backpressure
 * ------------
 * If the partition queue is full, router.route() throws TaskRejectedException.
 * We write a RESP error immediately and continue — the client sees the error
 * for that one command and can retry.  The connection itself stays alive.
 *
 * Keyless commands (PING, INFO, …)
 * ---------------------------------
 * Commands with no key argument use "__keyless__" as the routing key.
 * They always land on the same partition slot (consistent hash), which is fine
 * because keyless commands don't race with each other in any meaningful way.
 */
public class RedisClientHandler implements Runnable {

    private static final long   OFFER_TIMEOUT_MS = 100;
    private static final String ERR_OVERLOADED   = "-ERR server overloaded, try again\r\n";
    private static final String ERR_SHUTDOWN     = "-ERR server shutting down\r\n";

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

            RespParser parser = new RespParser(clientSocket.getInputStream());
            RespWriter  writer = new RespWriter(clientSocket.getOutputStream());

            while (true) {

                RespCommand command = parser.readCommand();
                if (command == null) break;   // client disconnected

                String[] args = command.getArguments();
                String   verb = args[0].toUpperCase();
                String   key  = (args.length > 1) ? args[1] : "__keyless__";

                // One future per command — the partition worker completes it,
                // this thread joins on it.  Ordering is therefore determined
                // by the order we call join(), which equals the order commands
                // arrived from the client.
                CompletableFuture<CommandResult> future = new CompletableFuture<>();

                try {

                    router.route(
                            verb,
                            key,
                            () -> {
                                // ---- Runs on a partition worker thread ----
                                CommandResult result;
                                try {
                                    result = commandHandler.execute(command);
                                } catch (Exception e) {
                                    result = CommandResult.error(
                                            "ERR internal: " + e.getMessage());
                                }

                                // AOF: persist mutating commands only
                                if (isWriteCommand(verb)) {
                                    try {
                                        database.persist(command);
                                    } catch (Exception e) {
                                        System.err.println(
                                                "AOF write failed: " + e.getMessage());
                                    }
                                }

                                // Unblock the connection worker — it will write
                                // the response in its own thread, in order.
                                future.complete(result);
                            },
                            OFFER_TIMEOUT_MS,
                            TimeUnit.MILLISECONDS
                    );

                    // Block until the partition worker signals completion.
                    // This connection worker thread is the ONLY writer to this
                    // socket, so no synchronisation on writer is needed.
                    CommandResult result = future.join();
                    writer.write(result);

                } catch (TaskRejectedException e) {

                    // Queue full or shutting down — respond immediately and
                    // keep the connection alive for the next command.
                    String errMsg = e.getMessage().contains("shutting down")
                            ? ERR_SHUTDOWN
                            : ERR_OVERLOADED;
                    try {
                        clientSocket.getOutputStream().write(errMsg.getBytes());
                    } catch (IOException ignored) {}

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

        } catch (IOException e) {
            System.out.println("Client connection error: " + e.getMessage());
        } finally {
            try { clientSocket.close(); } catch (IOException ignored) {}
        }
    }

    // ----------------------------------------------------------------
    // Write-command classification (AOF persistence)
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
