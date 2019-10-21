package stream;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TopN {
    public static void main(String[] args) throws IOException {
        String filePath = args[0];
        String outputFile = args[1] + "_resoult.csv";

        Map<String, Long> map = new BufferedReader(new FileReader(filePath))
                .lines()
                .parallel()
                .map(TopN::toDomain)
                .collect(Collectors.groupingBy(domain -> domain, Collectors.counting()));
        List<String> dlist = map.entrySet()
                                    .stream()
                                    .parallel()
                                    .sorted((o1, o2) -> o2.getValue().compareTo(o1.getValue()))
                                    .limit(10)
                                    .map(x -> x.getKey() + "," + x.getValue())
                                    .collect(Collectors.toList());

        output(dlist, outputFile);
    }
    private static void output(List<String> dlist, String outputFile) throws IOException {
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputFile));
        for (String s : dlist) {
            bufferedWriter.append(s).append(System.lineSeparator());
        }
        bufferedWriter.flush();
        bufferedWriter.close();
    }

    public static String toDomain(String line) {
        return line.split("\t")[3].split(" ")[0];
    }
}
