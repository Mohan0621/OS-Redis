import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;

public class AofLogger {

    private final String filePath;
    private final BufferedOutputStream output;

    public AofLogger(String filePath) throws IOException {
        this.filePath = filePath;
        output = new BufferedOutputStream(
                new FileOutputStream(filePath, true) // append=true so existing data is preserved
        );
    }

    // Appends raw RESP bytes from a client command to the AOF file
    public synchronized void append(byte[] data) throws IOException {
        output.write(data);
        output.flush();
    }

    // Replays all commands stored in the AOF file through the given CommandHandler.
    // Called once on startup before accepting clients.
    public void replay(CommandHandler commandHandler) throws IOException {

        if (!Files.exists(Paths.get(filePath))) {
            System.out.println("No AOF file found, starting fresh.");
            return;
        }

        System.out.println("Replaying AOF log...");

        int count = 0;

        try (FileInputStream fis = new FileInputStream(filePath)) {

            RespParser parser = new RespParser(fis);

            while (true) {

                RespCommand command = parser.readCommand();

                if (command == null) {
                    break; // end of file
                }

                // Execute without re-logging (recovering=true is set on the database before this call)
                commandHandler.execute(command);
                count++;
            }
        }

        System.out.println("AOF replay complete — " + count + " commands restored.");
    }

    public synchronized void close() throws IOException {
        output.close();
    }
}
