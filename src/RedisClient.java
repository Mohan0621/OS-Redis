import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class RedisClient {

    public static void main(String[] args) throws Exception {

        Socket socket = new Socket("localhost", 6379);

        OutputStream output = socket.getOutputStream();

        String command =
                "*3\r\n" +
                "$3\r\n" +
                "SET\r\n" +
                "$4\r\n" +
                "name\r\n" +
                "$5\r\n" +
                "Mohan\r\n";

        output.write(command.getBytes(StandardCharsets.UTF_8));
        output.flush();

        socket.close();
    }
}