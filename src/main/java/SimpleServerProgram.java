import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class SimpleServerProgram {

    public static void main(String args[]) {
        try {
            ServerSocket listener = new ServerSocket(4398);
            System.out.println("Server is waiting to accept user...");

            // Accept client connection request
            // Get new Socket at Server.
            Socket socketOfServer = listener.accept();
            System.out.println("Accept a client!");

            // Open input and output streams

            DataOutputStream outputStream = new DataOutputStream(socketOfServer.getOutputStream());

            //  Input stream at Client (Receive data from the server).
            DataInputStream inputStream = new DataInputStream(socketOfServer.getInputStream());

            FileOutputStream fileOutputStream = new FileOutputStream("test.txt");

            // Read data to the server (sent from client).

            long numberServers = inputStream.readLong();
            List<String> servers = new ArrayList<>();
            int serverId = 0;
            for (int i = 0; i < numberServers; i++) {
                servers.add(inputStream.readUTF());
                if (inputStream.readBoolean()) {
                    serverId = i;
                }
            }
            for (int i = 0; i < numberServers; i++) {
                System.out.println(servers.get(i) + (serverId == i ? "Me" : ""));
            }

            receiveSplit(inputStream, fileOutputStream);

            System.out.println("Sending OK");

            outputStream.writeUTF(">> OK");
            outputStream.flush();


        } catch (IOException e) {
            System.out.println(e);
            e.printStackTrace();
        }
        System.out.println("Sever stopped!");
    }

    private static void receiveSplit(DataInputStream inputStream, FileOutputStream fileOutputStream) throws IOException {
        int bytesRead = 0;
        long splitSize = inputStream.readLong();
        byte[] buffer = new byte[8192];
        while (splitSize > 0 && (bytesRead = inputStream.read(buffer, 0, (int) Math.min(buffer.length, splitSize))) != -1) {
            fileOutputStream.write(buffer, 0, bytesRead);
            splitSize -= bytesRead;
        }
        fileOutputStream.close();
    }
}