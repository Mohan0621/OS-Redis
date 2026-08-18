import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RedisServer {

    public static void main(String[] args) throws IOException {

        AofLogger aofLogger = new AofLogger("redis.aof");

        RedisDatabase database = new RedisDatabase(aofLogger);

        CommandHandler commandHandler = new CommandHandler(database);

        // --- AOF Replay on startup ---
        // Set recovering=true so commands replayed from the AOF file
        // are not written back to the AOF (that would double-log everything)
        database.setRecovering(true);
        aofLogger.replay(commandHandler);
        database.setRecovering(false);
        // --- End of replay ---

        ServerSocket serverSocket = new ServerSocket(6379);

        ExecutorService clientExecutor =
                Executors.newFixedThreadPool(4);

        ScheduledExecutorService expirationScheduler =
                Executors.newScheduledThreadPool(1);

        expirationScheduler.scheduleAtFixedRate(
                database::removeExpiredKeys,
                1,
                1,
                TimeUnit.SECONDS
        );

        System.out.println("Redis server started on port 6379");

        while (true) {

            Socket clientSocket = serverSocket.accept();

            System.out.println("Client connected!");

            clientExecutor.execute(
                    new RedisClientHandler(clientSocket, commandHandler, database)
            );
        }
    }
}
