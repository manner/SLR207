import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;

public class WordCountSingleThreaded {
    public static void main(String[] args) throws IOException {
        String filename = "/Users/moritzmanner/Projects/SLR207/src/main/java/CC-MAIN-20220116093137-20220116123137-00001.warc.wet";
        Scanner in = new Scanner(new File(filename));
        BufferedReader objReader = new BufferedReader(new FileReader(filename));

        Map<String, Integer> word_count = new HashMap<>();

        long timeStart = System.currentTimeMillis();

//        String strCurrentLine;
//        while ((strCurrentLine = objReader.readLine()) != null) {
//            String[] test = strCurrentLine.split(" ");
//            for (String a : test) {
//                word_count.merge(a, 1, Integer::sum);
//            }
//        }


        while (in.hasNext()) {
            word_count.merge(in.next(), 1, Integer::sum);
        }

        long timeCountOccurrences = System.currentTimeMillis();

        List<Map.Entry<String, Integer>> sorted = word_count.entrySet().stream()
                .sorted(Collections.reverseOrder(Map.Entry.<String, Integer>comparingByValue()).thenComparing(Map.Entry.comparingByKey()))
                .limit(50)
                .collect(Collectors.toList());

        long timeSorting = System.currentTimeMillis();

        sorted.forEach(x -> System.out.println(x.getKey() + " " + x.getValue()));

        System.out.println("Time Count Occurrences: " + (timeCountOccurrences - timeStart) + "ms");
        System.out.println("Time Sorting: " + (timeSorting - timeCountOccurrences) + "ms");
        System.out.println("Time Total: " + (timeSorting - timeStart) + "ms");

//        in.close();
    }
}