import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
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

//            for (int i = 0; i < numberServers; i++) {
//                System.out.println(servers.get(i) + (serverId == i ? " Me" : ""));
//            }

            long startReceivingSplit = System.currentTimeMillis();
            receiveSplit(inputStream, fileOutputStream);
            long endReceivingSplit = System.currentTimeMillis();
            System.out.println("Time Receiving Split: " + (endReceivingSplit - startReceivingSplit) + "ms");

            Receiver receiver = new Receiver(servers, serverId, listener);
            Thread receiver_thread = new Thread(receiver);
            receiver_thread.start();

            // Start sending key value pairs to other servers
            Sender sender = new Sender(servers, serverId, splitFile);
            sender.run();

            receiver_thread.join();
            Map<String, Integer> word_count_received = receiver.get_word_count();
            Map<String, Integer> word_count_own = sender.get_word_count();


            // reduce
            long startMergingWordCounts = System.currentTimeMillis();

            word_count_own.forEach((key, value) -> word_count_received.merge(key, value, Integer::sum));
            long endMergingWordCounts = System.currentTimeMillis();
            System.out.println("Time Merging Wordcounts: " + (endMergingWordCounts - startMergingWordCounts) + "ms");

            long startSendingWordCounts = System.currentTimeMillis();
            outputStream.writeInt(word_count_received.size());
            for (Map.Entry<String, Integer> entry : word_count_received.entrySet()) {
                outputStream.writeUTF(entry.getKey());
                outputStream.writeInt(entry.getValue());
//                System.out.println(serverId + "   " + entry.getKey() + ": " + entry.getValue());
            }
            long endSendingWordCounts = System.currentTimeMillis();
            System.out.println("Time Sending Wordcounts: " + (endSendingWordCounts - startSendingWordCounts) + "ms");

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
//        System.out.println(splitSize);
        byte[] buffer = new byte[8192];
        while (splitSize > 0 && (bytesRead = inputStream.read(buffer, 0, (int) Math.min(buffer.length, splitSize))) != -1) {
            fileOutputStream.write(buffer, 0, bytesRead);
//            System.out.println("'" + new String(buffer, StandardCharsets.UTF_8) + "'");
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
            long startReceivingWords = System.currentTimeMillis();

            for (int i = 0; i < servers.size(); i++) {
                if (i == serverId) {
                    finished.add(true);
                    inputStreams.add(new DataInputStream(null));
                    continue;
                }
                finished.add(false);

                // Get new Socket at Server.
                Socket receiverSocket = listener.accept();
                sockets.add(receiverSocket);
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
                            int value = inputStreams.get(i).readInt();
                            word_count.merge(key, value, Integer::sum);
                        }
                    }
                }

            }
            long endReceivingWords = System.currentTimeMillis();
            System.out.println("Time Receiving Words: " + (endReceivingWords - startReceivingWords) + "ms");

        } catch (IOException e) {
            e.printStackTrace();
        }
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
    private final List<Map<String, Integer>> word_counts = new ArrayList<Map<String, Integer>>();

    public Sender(List<String> servers, int serverId, String splitFile) {
        this.servers = servers;
        this.serverId = serverId;
        this.splitFile = splitFile;
    }

    public void run() {
        try {
            long startConnections = System.currentTimeMillis();

            // Create connection to other addresses
            for (int i = 0; i < servers.size(); i++) {
                word_counts.add(new HashMap<>());
                if (i == serverId) {
                    sockets.add(new Socket());
                    connections.add(new DataOutputStream(null));
                    continue;
                }
                Socket socket = new Socket(servers.get(i), 4398);
                sockets.add(socket);
                DataOutputStream stream = new DataOutputStream(socket.getOutputStream());
                connections.add(stream);


            }
            long startSending = System.currentTimeMillis();
            System.out.println("Time Connecting to other servers: " + (startSending - startConnections) + "ms");

            BufferedReader objReader = new BufferedReader(new InputStreamReader(new FileInputStream(splitFile), StandardCharsets.UTF_8));
//            BufferedReader objReader = null;
//            objReader = new BufferedReader(new FileReader(splitFile));
            String strCurrentLine;
            while ((strCurrentLine = objReader.readLine()) != null) {
                String[] keys = strCurrentLine.split(" ");
                for (String key : keys) {
//                    System.out.println(key);
                    if (key.getBytes().length > 1000) {
//                        System.out.println(key);
                        continue;
                    }
                    int serverToSend = Math.abs(hash(key)) % servers.size();
                    word_counts.get(serverToSend).merge(key, 1, Integer::sum);

                }
            }
            for (int i = 0; i < servers.size(); i++) {
                if (serverId == i) {
                    continue;
                }
                for (Map.Entry<String, Integer> entry : word_counts.get(i).entrySet()) {
                    connections.get(i).writeUTF(entry.getKey());
                    connections.get(i).writeInt(entry.getValue());
//                System.out.println(serverId + "   " + entry.getKey() + ": " + entry.getValue());
                }
                connections.get(i).writeUTF(SimpleServerProgram.FINISHED_KEYWORD);
            }
            long finishedSending = System.currentTimeMillis();
            System.out.println("Time Sending to other servers: " + (finishedSending - startSending) + "ms");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    Map<String, Integer> get_word_count() {
        return word_counts.get(serverId);
    }
}