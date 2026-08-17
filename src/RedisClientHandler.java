import java.io.IOException;
import java.net.socket;

public class RedisClientHandler implements Runnable {
    private final Socket clientSocket;
    private final CommandHandler CommandHandler;
    RedisClientHandler(Socket clientSocket,CommandHandler commandHandler){
        this.clientSocket=clientSocket;
        this.commandHandler=commandHandler;
    }

    @Override
    public void run(){
        try{
            RespParser parser= new RespParser(clientSocket.getInputStream());
            RespWriter writer =new RespWriter(clientSocket.getOutputStream());

            String response = commandHandler.execute(command);

            while(true){
                String[] command = parser.readCommand();
                if(command==null){
                    break;
                }

                if(command[0].equalsIggnoreCase("SET")){
                    writer.writeSimpleString(response);
                }
                else if(command[0].equalsIggnoreCase("GET")){
                    writer.writeBulkString(response.equals("(nil)")?null:response);
                }
                else if (command[0].equalsIggnoreCase("DEL")){
                    writer.writeInteger(Integer.parserInt(response));
                }
                else{
                    writer.writeError(response);
                }
            } catch (IOException e) {

            System.out.println(
                    "Client connection error: "
                            + e.getMessage()
            );

        } finally {

            try {
                clientSocket.close();
            } 
            catch (IOException ignored) {
            }

        }
    }


}
