import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class SimpleClient {

    public static void main(String[] args) {

        // Server Host
        final String serverHost = "tp-1a226-";
//        final String[] servers = {"06"};
        final String[] servers = {"06", "07", "08"};
        final String domain = ".enst.fr";
        final String filename = "/Users/moritzmanner/Projects/SLR207/src/main/java/split";

        for (int i = 0; i < servers.length; i++) {
            try {
                // Send a request to connect to the server is listening
                // on machine 'localhost' port 1234.
                String serverName = serverHost + servers[i] + domain;
                Socket socketOfClient = new Socket(serverName, 4398);

                // Create output stream at the client (to send data to the server)
                DataOutputStream outputStream = new DataOutputStream(socketOfClient.getOutputStream());

//                 Input stream at Client (Receive data from the server).
                DataInputStream inputStream = new DataInputStream(socketOfClient.getInputStream());

                // Read split file
                File split = new File(filename + (i + 1) + ".txt");
                FileInputStream fileInputStream = new FileInputStream(split);

                // Send server addresses to all
                outputStream.writeLong(servers.length);
                for (int serverId = 0; serverId < servers.length; serverId++) {
                    outputStream.writeUTF(serverHost + servers[serverId] + domain);
                    outputStream.writeBoolean(i == serverId);
                }

                // Send splits to each server
                outputStream.writeLong(split.length());
                int count;
                byte[] buffer = new byte[8192]; // or 4096, or more
                while ((count = fileInputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, count);
                    outputStream.flush();
                }
                fileInputStream.close();

                outputStream.writeUTF("QUIT");
                outputStream.flush();

                // Read data sent from the server.
                // By reading the input stream of the Client Socket.
                String responseLine;
                while ((responseLine = inputStream.readUTF()) != null) {
                    System.out.println("Server" + i + ": " + responseLine);
                    if (responseLine.indexOf("OK") != -1) {
                        break;
                    }
                }

                outputStream.close();
                inputStream.close();
                socketOfClient.close();
            } catch (UnknownHostException e) {
                System.err.println("Trying to connect to unknown host: " + e);
            } catch (IOException e) {
                System.err.println("IOException:  " + e);
            }
        }
    }
}