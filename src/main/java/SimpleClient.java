import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SimpleClient {

    public static void main(String[] args) {

        // Server Host
        final String serverHost = "tp-1a226-";
//        final String[] servers = {"06"};
        final String[] servers = {"06", "07", "08", "12", "14", "16", "17", "20", "22", "23"};
        final String domain = ".enst.fr";
        final String filename = "/Users/moritzmanner/Projects/SLR207/src/main/java/rename.wet";
        long timeStart = System.currentTimeMillis();

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
//            TimeUnit.SECONDS.sleep(1);

            System.out.println("Sending Splits to each server.");

            // create splits


//            int count = 0;
            List<Thread> splitSenders = new ArrayList<>();
            for (int i = 0; i < servers.length; i++) {

                SplitSender splitSender = new SplitSender(outputStream.get(i), i, filename, servers.length);
//                receiverList.add(receiver);
                Thread splitSenderThread = new Thread(splitSender);
                splitSenders.add(splitSenderThread);
                splitSenderThread.start();
            }

            for (Thread thread : splitSenders) {
                thread.join();
            }

            long sentSplits = System.currentTimeMillis();
            System.out.println("Time Sending Splits: " + (sentSplits - timeStart) + "ms");


            for (int i = 0; i < servers.length; i++) {
                inputStream.add(new DataInputStream(socketOfClient.get(i).getInputStream()));
            }
            HashMap<String, Integer> word_count = new HashMap<>();
            List<WordCountReceiver> receiverList = new ArrayList<>();
            List<Thread> receiverThreads = new ArrayList<>();
            System.out.println("Receiving word_counts");

            // Starting receiver threads
            for (int i = 0; i < servers.length; i++) {
                WordCountReceiver receiver = new WordCountReceiver(inputStream.get(i), i);
                receiverList.add(receiver);
                Thread receiverThread = new Thread(receiver);
                receiverThreads.add(receiverThread);
                receiverThread.start();
            }

            for (int i = 0; i < servers.length ; i++) {
                receiverThreads.get(i).join();
                receiverList.get(i).getWord_count().forEach((key, value) -> word_count.merge(key, value, Integer::sum));
            }

            long timeCountOccurrences = System.currentTimeMillis();
            System.out.println("Time Count Occurrences: " + (timeCountOccurrences - sentSplits) + "ms");

            for (int i = 0; i < servers.length; i++) {
                outputStream.get(i).close();
                inputStream.get(i).close();
                socketOfClient.get(i).close();
            }
            long timeClosing = System.currentTimeMillis();
            System.out.println("Time Closing: " + (timeClosing - timeCountOccurrences) + "ms");

            System.out.println("Elements received: " + word_count.size());

            List<Map.Entry<String, Integer>> top50 = word_count.entrySet().stream()
                    .sorted(Collections.reverseOrder(Map.Entry.<String, Integer>comparingByValue()).thenComparing(Map.Entry.comparingByKey()))
                    .limit(50)
                    .collect(Collectors.toList());
//                    .forEach(x -> System.out.println(x.getKey() + " " + x.getValue()));

            long timeSorting = System.currentTimeMillis();

            System.out.println("Time Sorting: " + (timeSorting - timeClosing) + "ms");
            System.out.println("Time Total: " + (timeSorting - timeStart) + "ms");

        } catch (UnknownHostException e) {
            System.err.println("Trying to connect to unknown host: " + e);
        } catch (IOException e) {
            System.err.println("IOException:  " + e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}


class SplitSender implements Runnable {

    private DataOutputStream outputStream;
    private int serverId;
    private RandomAccessFile raf;
    private String filename;
    private long fileSize;
    private long bytesPerSplit;
    private long remainingBytes;
    private int serverLength;

    public SplitSender(DataOutputStream outputStream, int i, String filename, int serverLength) throws IOException {
        this.outputStream = outputStream;
        this.serverId = i;
        this.filename = filename;
        this.raf = new RandomAccessFile(filename, "r");
        this.serverLength = serverLength;
        fileSize = raf.length();
        bytesPerSplit = fileSize / serverLength;
        remainingBytes = fileSize % serverLength;
    }

    public void run() {
        long splitSize;
        if (serverId == (serverLength - 1)) {
            splitSize = bytesPerSplit + remainingBytes;
        } else {
            splitSize = bytesPerSplit;
        }
        try {
            raf.seek(bytesPerSplit * serverId);
            outputStream.writeLong(splitSize);

            byte[] buffer = new byte[8192]; // or 4096, or more
            int bytesRead;

            while (splitSize > 0 && (bytesRead = raf.read(buffer, 0, (int) Math.min(buffer.length, splitSize))) != -1) {
                outputStream.write(buffer, 0, bytesRead);
//                    System.out.println(i);
//                    System.out.println("'" + new String(buffer, StandardCharsets.UTF_8) + "'");
                outputStream.flush();
                splitSize -= bytesRead;
            }
            raf.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

class WordCountReceiver implements Runnable {

    private DataInputStream inputStream;
    private int serverId;
    private HashMap<String, Integer> word_count = new HashMap<>();

    public WordCountReceiver(DataInputStream inputStream, int i) {
        this.inputStream = inputStream;
        this.serverId = i;
    }

    public void run() {
        System.out.println("Reading from " + serverId);
        // Read data sent from the server.
        // By reading the input stream of the Client Socket.
        try {
            while (inputStream.available() <= 0) {
            }
            System.out.println("Available from " + serverId);
            int keys_available = inputStream.readInt();
            for (int j = 0; j < keys_available; j++) {
                String key = inputStream.readUTF();
                int value = inputStream.readInt();
                word_count.put(key, value);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public HashMap<String, Integer> getWord_count() {
        return word_count;
    }
}
