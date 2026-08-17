/*
    Normal 2 server talk using websockets like some protocals TCP
        tcp which gives us a reliable communication channel
        so we have seversocket which waits for clients and socket which takes care of communication btween sever and client


import java.io.IOException;
import java.net.ServerSocket;//import ServerSocket
import java.net.Socket;//import Socket

public class RedisServer {

    public static void main(String[] args) throws IOException {

        ServerSocket serverSocket = new ServerSocket(6379);// like saying i want listen on port 6379

        System.out.println("Redis server started on port 6379");
        System.out.println("Waiting for client...");

        Socket clientSocket = serverSocket.accept();//this is where we accept the client connection and returns Socket(which responsible for communication between 2 devices)

        System.out.println("Client connected!");

        clientSocket.close();//here we are closing the scoket 
        serverSocket.close();//here we are closing the ServerScoket
    }
}

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.InputStream;

public class RedisServer{
    public static void main(String [] args) throws IOException {
        ServerSocket serverSocket = new ServerSocket(6379);//takes a single input port number to initialise the server
        System.out.println("Redis Server is Started to listen on 6379");
        System.out.println("Waiting for client...");

        Socket clientSocket = serverSocket.accept();
        InputStream input = clientSocket.getInputStream();
        int data;//but why the int data? java InputStream.read() returns an int
        while((data = input.read()) != -1){//input.read() asks for a byte, returns its numeric value, waits if no data
        //if the client closes the connection it returns -1
            System.out.println((char) data);
        }
        clientSocket.close();
        serverSocket.close();
    }
}*/


//RESP(Redis Serialization Protocol) it used for comunicating with redis server since it dosnt know set get del
// it gives us rules normal it dosnt know it what command is this how many arguments where dose a argument end what type of data is sent ?
/*
    set name mohan
    mean it will tell
    *3-3 elements
    $3-next string is 3 bytes
    SET
    $4-next string is 4 bytes
    name
    $5-next string is 5 bytes
    Mohan

    each resp ends with \r\n  uses crlf carraige return and line feed
    \r=carriage return
    \n=line feed
 

// RESP Parser
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class RedisServer {

    public static void main(String[] args) throws IOException {

        ServerSocket serverSocket = new ServerSocket(6379);

        System.out.println("Redis server started on port 6379");

        Socket clientSocket = serverSocket.accept();

        System.out.println("Client connected!");

        RespParser parser =
                new RespParser(clientSocket.getInputStream());

        String[] command = parser.readCommand();

        for (String part : command) {
            System.out.println(part);
        }

        clientSocket.close();
        serverSocket.close();
    }
}
*

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Server;

public class RedisServer{
    public static void main(String[] args){
        SeverSocket serverSocket = new ServerScoket(6379);
        RedisDatabase database =new RedisDatabase();
        CommandHandler handler=new CommandHandler();
        System.out.println("Redis server Started on port 6379");
        RespParser praser=new RespParser(scoket,getInputStream());
        String[] command =parser.readCommand();
        String response =handler.execute(command);
        System.out.println("Response : "+response);
        socket.close();
        serverSocket.close();
    }


}
*
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class RedisServer {

    public static void main(String[] args) throws IOException {

        ServerSocket serverSocket = new ServerSocket(6379);

        RedisDatabase database = new RedisDatabase();
        CommandHandler handler = new CommandHandler(database);

        System.out.println("Redis server started on port 6379");

        while (true) {

            Socket clientSocket = serverSocket.accept();

            System.out.println("Client connected!");

            RespParser parser =
                    new RespParser(clientSocket.getInputStream());

            RespWriter writer =
                    new RespWriter(clientSocket.getOutputStream());

            while (true) {

                String[] command = parser.readCommand();

                if (command == null) {
                    break;
                }

                String response = handler.execute(command);

                if (command[0].equalsIgnoreCase("SET")) {
                    writer.writeSimpleString(response);
                }
                else if (command[0].equalsIgnoreCase("GET")) {
                    writer.writeBulkString(
                            response.equals("(nil)") ? null : response
                    );
                }
                else if (command[0].equalsIgnoreCase("DEL")) {
                    writer.writeInteger(Integer.parseInt(response));
                }
                else {
                    writer.writeError(response);
                }
            }

            clientSocket.close();
        }
    }
}*/

import java.io.IOException;
import java.net.ServerScoket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
public class RedisServer{
    public static void main(String[] args) throw IOException{
        ServerScoket serverSocket=new SeverSocket(6379);
        RedisDatabase database = new RedisDatabase();
        CommandHandler commandHandler =new CommandHandler(database);
        ExecutorService executor=Executors.newFixedThreadPool(4);
        System,out.println("Redis Server is at 6379");

        while(true){
            Socket clientSocket=serverSocket.accept();
            System.out.println("Client connected!");
            RedisClientHandler clientHandler = new RedisClientHandler(clientSocket,commandHandler);
            executor.execute(clientHandler);//submit method used when we need to send the result to client since we dont need that we will use execute method
            clientHandler.start();
        }
    }
}




