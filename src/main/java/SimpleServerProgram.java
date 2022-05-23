import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.hash;


public class SimpleServerProgram {
    public static String FINISHED_KEYWORD = "---!@FINISHED@!---";


    public static void main(String args[]) throws IOException {
        ServerSocket listener = null;
        Socket socketOfServer = null;
        try {
            listener = new ServerSocket(4398);
            System.out.println("Server is waiting to accept user...");

            // Accept client connection request
            // Get new Socket at Server.
            socketOfServer = listener.accept();
            System.out.println("Accept a client!");

            // Open input and output streams
            DataOutputStream outputStream = new DataOutputStream(socketOfServer.getOutputStream());
            //  Input stream at Client (Receive data from the server).
            DataInputStream inputStream = new DataInputStream(socketOfServer.getInputStream());
            String splitFile = "split.txt";
            FileOutputStream fileOutputStream = new FileOutputStream(splitFile);

            // Receive all server addresses
            long numberServers = inputStream.readLong();
            List<String> servers = new ArrayList<>();

            int serverId = 0;
            for (int i = 0; i < numberServers; i++) {
                String serverName = inputStream.readUTF();
                servers.add(serverName);


                if (inputStream.readBoolean()) {
                    serverId = i;
                }
            }

            for (int i = 0; i < numberServers; i++) {
                System.out.println(servers.get(i) + (serverId == i ? " Me" : ""));
            }

            System.out.println(serverId + " receiving split from client");
            receiveSplit(inputStream, fileOutputStream);

            Receiver receiver = new Receiver(servers, serverId, listener);
            Thread receiver_thread = new Thread(receiver);
            receiver_thread.start();

            System.out.println("Started receiver.");

            // Start sending key value pairs to other servers
            Sender sender = new Sender(servers, serverId, splitFile);
            sender.run();

            receiver_thread.join();
            Map<String, Integer> word_count_received = receiver.get_word_count();
            Map<String, Integer> word_count_own = sender.get_word_count();

            // reduce
            word_count_own.forEach((key, value) -> word_count_received.merge(key, value, Integer::sum));

            for (Map.Entry<String, Integer> entry : word_count_received.entrySet()) {
                System.out.println(serverId + "   " + entry.getKey() + ": " + entry.getValue());
            }

            System.out.println("Sending OK");

            outputStream.writeUTF(">> OK");
            outputStream.flush();


        } catch (IOException | InterruptedException e) {
            System.out.println(e);
            socketOfServer.close();
            listener.close();
            e.printStackTrace();
        }
        System.out.println("Server stopped!");
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

    private static int hash(String key) {
        return key.hashCode();
    }
}

class Receiver implements Runnable {
    private final List<String> servers;
    private final int serverId;
    private final ServerSocket listener;
    private final List<Socket> sockets = new ArrayList<>();
    private final List<Boolean> finished = new ArrayList<>();
    private final List<DataInputStream> inputStreams = new ArrayList<>();
    private Map<String, Integer> word_count = new HashMap<>();


    public Receiver(List<String> servers, int serverId, ServerSocket listener) {
        this.servers = servers;
        this.serverId = serverId;
        this.listener = listener;
    }

    public void run() {
        // Receive keys from other servers
        try {
            for (int i = 0; i < servers.size(); i++) {
                if (i == serverId) {
                    finished.add(true);
                    inputStreams.add(new DataInputStream(null));
                    continue;
                }
                finished.add(false);
//                ServerSocket receiver = new ServerSocket(4398);
                System.out.println(serverId + " is waiting to accept sender...");

                // Get new Socket at Server.
                Socket receiverSocket = listener.accept();
                sockets.add(receiverSocket);

                System.out.println("Accept a sender!");
                DataInputStream receiverStream = new DataInputStream(receiverSocket.getInputStream());
                inputStreams.add(receiverStream);
            }
            while (finished.contains(false)) {
                for (int i = 0; i < servers.size(); i++) {
                    if (i == serverId) {
                        continue;
                    }
                    if (inputStreams.get(i).available() > 0) {
                        String key = inputStreams.get(i).readUTF();
                        if (SimpleServerProgram.FINISHED_KEYWORD.equals(key)) {
                            finished.set(i, true);
                        } else {
                            word_count.merge(key, 1, Integer::sum);
                            System.out.println(serverId + " received: " + key + " total: " + word_count.get(key));
                        }
                    }
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Stopping thread");
    }

    Map<String, Integer> get_word_count() {
        return word_count;
    }
}

class Sender implements Runnable {
    private final List<DataOutputStream> connections = new ArrayList<>();
    private final List<Socket> sockets = new ArrayList<>();
    private final List<String> servers;
    private final int serverId;
    private final String splitFile;
    private Map<String, Integer> word_count = new HashMap<>();

    public Sender(List<String> servers, int serverId, String splitFile) {
        this.servers = servers;
        this.serverId = serverId;
        this.splitFile = splitFile;
    }

    public void run() {
        System.out.println("thread is running...");
        try {
            // Create connection to other addresses
            for (int i = 0; i < servers.size(); i++) {
                if (i == serverId) {
                    sockets.add(new Socket());
                    connections.add(new DataOutputStream(null));
                    continue;
                }
                Socket socket = new Socket(servers.get(i), 4398);
                sockets.add(socket);
                DataOutputStream stream = new DataOutputStream(socket.getOutputStream());
                connections.add(stream);
                System.out.println("Sender connected to the receiver.");

            }
            System.out.println(serverId + " has " + sockets.size() + " socket connections.");

            BufferedReader objReader = null;
            objReader = new BufferedReader(new FileReader(splitFile));
            String strCurrentLine;
            while ((strCurrentLine = objReader.readLine()) != null) {
                String[] keys = strCurrentLine.split(" ");
                for (String key : keys) {
                    int serverToSend = hash(key) % servers.size();
                    if (serverToSend == serverId) {
                        System.out.println(key);
                        word_count.merge(key, 1, Integer::sum);
                        continue;
                    }
                    System.out.println(serverId + " about to send data to " + serverToSend);
                    if (connections.get(serverToSend) != null) {
                        connections.get(serverToSend).writeUTF(key);
                    } else {
                        System.out.println(serverToSend + " connection is empty.");
                    }
                }
            }
            for (int i = 0; i < servers.size(); i++) {
                if (serverId == i) {
                    continue;
                }
                connections.get(i).writeUTF(SimpleServerProgram.FINISHED_KEYWORD);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    Map<String, Integer> get_word_count() {
        return word_count;
    }
}