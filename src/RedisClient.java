import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class RedisClient {

    public static void main(String[] args) throws Exception {

        Socket socket = new Socket("localhost", 6379);

        OutputStream output = socket.getOutputStream();
        InputStream input = socket.getInputStream();

        send(output,
                "*3\r\n" +
                "$3\r\n" +
                "SET\r\n" +
                "$4\r\n" +
                "name\r\n" +
                "$5\r\n" +
                "Mohan\r\n");

        System.out.println("SET response:");
        readResponse(input);

        send(output,
                "*2\r\n" +
                "$3\r\n" +
                "GET\r\n" +
                "$4\r\n" +
                "name\r\n");

        System.out.println("GET response:");
        readResponse(input);

        send(output,
                "*2\r\n" +
                "$3\r\n" +
                "DEL\r\n" +
                "$4\r\n" +
                "name\r\n");

        System.out.println("DEL response:");
        readResponse(input);

        send(output,
                "*2\r\n" +
                "$3\r\n" +
                "GET\r\n" +
                "$4\r\n" +
                "name\r\n");

        System.out.println("GET response:");
        readResponse(input);

        socket.close();
    }

    private static void send(
            OutputStream output,
            String command) throws Exception {

        output.write(command.getBytes(StandardCharsets.UTF_8));
        output.flush();
    }

    private static void readResponse(InputStream input)
            throws Exception {

        byte[] buffer = new byte[1024];

        int count = input.read(buffer);

        System.out.println(
                new String(buffer, 0, count, StandardCharsets.UTF_8)
        );
    }
}