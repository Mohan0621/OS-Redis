import java.io.IOException;
import java.net.Socket;

public class RedisClientHandler implements Runnable {

    private final Socket clientSocket;
    private final CommandHandler commandHandler;
    private final RedisDatabase database;

    public RedisClientHandler(
            Socket clientSocket,
            CommandHandler commandHandler,
            RedisDatabase database) {

        this.clientSocket = clientSocket;
        this.commandHandler = commandHandler;
        this.database = database;
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
                    break;
                }

                CommandResult result = commandHandler.execute(command);

                // Persist the raw RESP bytes to the AOF log *after* a successful execute
                // Only write-commands need persisting; reads (GET, TTL) are skipped
                String op = command.getArguments()[0].toUpperCase();
                if (op.equals("SET") || op.equals("DEL") || op.equals("INCR")) {
                    database.persist(command);
                }

                writer.write(result);
            }

        } catch (IOException e) {

            System.out.println("Client connection error: " + e.getMessage());

        } finally {

            try {
                clientSocket.close();
            } catch (IOException ignored) {
            }
        }
    }
}
