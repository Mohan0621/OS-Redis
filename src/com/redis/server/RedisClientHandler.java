package com.redis.server;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import com.redis.db.CommandHandler;
import com.redis.db.RedisDatabase;
import com.redis.db.CommandResult;
import com.redis.partition.CommandRouter;
import com.redis.protocol.RespParser;
import com.redis.protocol.RespType;
import com.redis.protocol.RespWriter;
import com.redis.protocol.RespCommand;
import com.redis.pool.TaskRejectedException;

/**
 * Handles one TCP client connection end-to-end.
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

                try {

                    CommandResult result;
                    if (("BLPOP".equals(verb) || "BRPOP".equals(verb)) && args.length >= 3) {
                        result = executeBlockingListPop(command, verb, args, key);
                    } else {
                        result = dispatchAndWait(command, verb, key);
                    }

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

    private CommandResult dispatchAndWait(RespCommand command, String verb, String key)
            throws InterruptedException {

        CompletableFuture<CommandResult> future = new CompletableFuture<>();

        router.route(
                verb,
                key,
                () -> {
                    CommandResult result;
                    try {
                        result = commandHandler.execute(command);
                    } catch (Exception e) {
                        result = CommandResult.error("ERR internal: " + e.getMessage());
                    }

                    if (shouldPersist(verb, result)) {
                        try {
                            database.persist(command);
                        } catch (Exception e) {
                            System.err.println("AOF write failed: " + e.getMessage());
                        }
                    }

                    future.complete(result);
                },
                OFFER_TIMEOUT_MS,
                TimeUnit.MILLISECONDS
        );

        return future.join();
    }

    /**
     * Executes BLPOP / BRPOP without ever blocking a partition worker or
     * holding a database semaphore permit. Waits on the connection-worker thread.
     */
    private CommandResult executeBlockingListPop(
            RespCommand command, String verb, String[] args, String routingKey)
            throws InterruptedException {

        CommandResult initial = dispatchAndWait(command, verb, routingKey);
        if (initial.getType() != RespType.NULL) {
            return initial;
        }

        double timeoutSeconds = Double.parseDouble(args[args.length - 1]);
        boolean indefinite = (timeoutSeconds == 0.0);
        long deadlineNanos = indefinite
                ? Long.MAX_VALUE
                : System.nanoTime() + (long) (timeoutSeconds * 1_000_000_000.0);

        String[] watchKeys = new String[args.length - 2];
        System.arraycopy(args, 1, watchKeys, 0, watchKeys.length);

        while (indefinite || System.nanoTime() < deadlineNanos) {
            CompletableFuture<Void> signal = new CompletableFuture<>();
            for (String wk : watchKeys) {
                database.registerListWaiter(wk, signal);
            }
            try {
                // Re-check after registering waiter so no push between check & register is missed.
                CommandResult retry = dispatchAndWait(command, verb, routingKey);
                if (retry.getType() != RespType.NULL) {
                    return retry;
                }

                long waitMs;
                if (indefinite) {
                    waitMs = 500L;
                } else {
                    long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
                    if (remainingMs <= 0) break;
                    waitMs = Math.min(remainingMs, 500L);
                }

                try {
                    signal.get(waitMs, TimeUnit.MILLISECONDS);
                } catch (TimeoutException ignored) {
                    // Loop to check deadline or retry
                } catch (ExecutionException ignored) {
                    // Should not occur
                }
            } finally {
                for (String wk : watchKeys) {
                    database.unregisterListWaiter(wk, signal);
                }
            }

            CommandResult popped = dispatchAndWait(command, verb, routingKey);
            if (popped.getType() != RespType.NULL) {
                return popped;
            }
        }

        return CommandResult.nil();
    }

    // ----------------------------------------------------------------
    // Write-command classification (AOF persistence)
    // ----------------------------------------------------------------

    private static boolean shouldPersist(String verb, CommandResult result) {
        if (result.getType() == RespType.ERROR) {
            return false;
        }
        if ("BLPOP".equals(verb) || "BRPOP".equals(verb)) {
            return result.getType() == RespType.ARRAY;
        }
        return isWriteCommand(verb);
    }

    private static boolean isWriteCommand(String verb) {
        return switch (verb) {
            case "SET", "DEL", "INCR", "DECR",
                 "MSET", "SETNX", "GETSET", "GETDEL", "GETEX", "APPEND",
                 "LPUSH", "RPUSH", "LPOP", "RPOP",
                 "LSET", "LREM", "LINSERT", "LTRIM", "BLPOP", "BRPOP",
                 "HSET", "HDEL", "HMSET", "HINCRBY", "HINCRBYFLOAT",
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
