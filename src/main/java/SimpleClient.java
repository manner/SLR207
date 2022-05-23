import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SimpleClient {

    public static void main(String[] args) {

        // Server Host
        final String serverHost = "tp-1a226-";
//        final String[] servers = {"06"};
        final String[] servers = {"06", "07", "08"};
        final String domain = ".enst.fr";
        final String filename = "/Users/moritzmanner/Projects/SLR207/src/main/java/split";
        try {
            List<DataOutputStream> outputStream = new ArrayList<>();
            List<DataInputStream> inputStream = new ArrayList<>();
            List<File> split = new ArrayList<>();
            List<FileInputStream> fileInputStream = new ArrayList<>();
            List<Socket> socketOfClient = new ArrayList<>();
            for (int i = 0; i < servers.length; i++) {
                // Send a request to connect to the server is listening
                // on machine 'localhost' port 1234.
                String serverName = serverHost + servers[i] + domain;
                socketOfClient.add(new Socket(serverName, 4398));

                // Create output stream at the client (to send data to the server)
                outputStream.add(new DataOutputStream(socketOfClient.get(i).getOutputStream()));

                // Input stream at Client (Receive data from the server).

                // Send server addresses to all
                outputStream.get(i).writeLong(servers.length);
                for (int serverId = 0; serverId < servers.length; serverId++) {
                    outputStream.get(i).writeUTF(serverHost + servers[serverId] + domain);
                    outputStream.get(i).writeBoolean(i == serverId);
                }
            }
            TimeUnit.SECONDS.sleep(1);

            System.out.println("Sending Splits to each server.");

            for (int i = 0; i < servers.length; i++) {

                // Read split file
                split.add(new File(filename + (i + 1) + ".txt"));
                fileInputStream.add(new FileInputStream(split.get(i)));

                // Send splits to each server
                outputStream.get(i).writeLong(split.get(i).length());
                int count;
                byte[] buffer = new byte[8192]; // or 4096, or more
                while ((count = fileInputStream.get(i).read(buffer)) != -1) {
                    outputStream.get(i).write(buffer, 0, count);
                    outputStream.get(i).flush();
                }
                fileInputStream.get(i).close();
                outputStream.get(i).writeUTF("QUIT");
                outputStream.get(i).flush();
            }
            for (int i = 0; i < servers.length; i++) {
                inputStream.add(new DataInputStream(socketOfClient.get(i).getInputStream()));

                // Read data sent from the server.
                // By reading the input stream of the Client Socket.
                String responseLine;
                while ((responseLine = inputStream.get(i).readUTF()) != null) {
                    System.out.println("Server" + i + ": " + responseLine);
                    if (responseLine.indexOf("OK") != -1) {
                        break;
                    }
                }

                outputStream.get(i).close();
                inputStream.get(i).close();
                socketOfClient.get(i).close();
            }
        } catch (UnknownHostException e) {
            System.err.println("Trying to connect to unknown host: " + e);
        } catch (IOException e) {
            System.err.println("IOException:  " + e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}