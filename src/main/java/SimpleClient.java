import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleClient {

    public static void main(String[] args) {

        // Server Host
        final String serverHost = "tp-1a226-";
//        final String[] servers = {"06"};
        final String[] servers = {"06", "07", "08"};
        final String domain = ".enst.fr";
        final String filename = "/Users/moritzmanner/Projects/SLR207/src/main/java/big.txt";
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

            RandomAccessFile raf = new RandomAccessFile(filename, "r");

            long fileSize = raf.length();
            long bytesPerSplit = fileSize / servers.length;
            long remainingBytes = fileSize % servers.length;
            int count = 0;

            for (int i = 0; i < servers.length; i++) {

                // Read split file
//                split.add(new File(filename + (i + 1) + ".txt"));
//                fileInputStream.add(new FileInputStream(split.get(i)));

                // Send splits to each server
                long splitSize;
                if (i == (servers.length - 1)) {
                    splitSize = bytesPerSplit + remainingBytes;
                } else {
                    splitSize = bytesPerSplit;
                }
                outputStream.get(i).writeLong(splitSize);

                byte[] buffer = new byte[8192]; // or 4096, or more
                int bytesRead;

                while (splitSize > 0 && (bytesRead = raf.read(buffer, 0, (int) Math.min(buffer.length, splitSize))) != -1) {
                    outputStream.get(i).write(buffer, 0, bytesRead);
//                    System.out.println(i);
//                    System.out.println("'" + new String(buffer, StandardCharsets.UTF_8) + "'");
                    outputStream.get(i).flush();
                    splitSize -= bytesRead;
                    count += bytesRead;
                }
//                while ((count = fileInputStream.get(i).read(buffer)) != -1) {
//                    outputStream.get(i).write(buffer, 0, count);
//                    outputStream.get(i).flush();
//                }
//                fileInputStream.get(i).close();
//                outputStream.get(i).writeUTF("QUIT");
//                outputStream.get(i).flush();
                //TODO remove
            }
            raf.close();
            long sentSplits = System.currentTimeMillis();
            System.out.println("Time Sending Splits: " + (sentSplits - timeStart) + "ms");


            for (int i = 0; i < servers.length; i++) {
                inputStream.add(new DataInputStream(socketOfClient.get(i).getInputStream()));
            }
            HashMap<String, Integer> word_count = new HashMap<>();
            List<Boolean> finished = new ArrayList<>(Collections.nCopies(servers.length, false));
            System.out.println("Receiving word_counts");
            while (finished.contains(false)) {
                for (int i = 0; i < servers.length; i++) {
                    if (finished.get(i)) {
                        continue;
                    }

                    // Read data sent from the server.
                    // By reading the input stream of the Client Socket.
                    DataInputStream current = inputStream.get(i);
                    if (current.available() > 0) {
                        int keys_available = current.readInt();
//                        System.out.println("Keys avaialbe " + keys_available);
                        for (int j = 0; j < keys_available; j++) {
                            String key = current.readUTF();
                            int value = current.readInt();
                            word_count.put(key, value);
                        }
                        finished.set(i, true);
                    }

//                    String responseLine;
//                    while ((responseLine = inputStream.get(i).readUTF()) != null) {
//                        System.out.println("Server" + i + ": " + responseLine);
//                        if (responseLine.indexOf("OK") != -1) {
//                            break;
//                        }
//                    }


                }
            }
            long timeCountOccurrences = System.currentTimeMillis();

            for (int i = 0; i < servers.length; i++) {
                outputStream.get(i).close();
                inputStream.get(i).close();
                socketOfClient.get(i).close();
            }

            word_count.entrySet().stream()
                    .sorted(Collections.reverseOrder(Map.Entry.<String, Integer>comparingByValue()).thenComparing(Map.Entry.comparingByKey()))
                    .limit(50)
                    .forEach(x -> System.out.println(x.getKey() + " " + x.getValue()));

            long timeSorting = System.currentTimeMillis();

            System.out.println("Time Count Occurrences: " + (timeCountOccurrences - sentSplits) + "ms");
            System.out.println("Time Sorting: " + (timeSorting - timeCountOccurrences) + "ms");
            System.out.println("Time Total: " + (timeSorting - timeStart) + "ms");

        } catch (UnknownHostException e) {
            System.err.println("Trying to connect to unknown host: " + e);
        } catch (IOException e) {
            System.err.println("IOException:  " + e);
        }

    }
}