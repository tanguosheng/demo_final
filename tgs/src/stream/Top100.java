package stream;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.reducing;

public class Top100 {


    public static void main(String[] args) throws IOException {

        String filePath = args[0];
        String outputFile = args[1]+"_resoult.csv";
        if (args.length == 3) {
            String parallelism = args[2];
            System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", parallelism);
        }


        top100WithSynchronizedHashMapLambada(filePath, outputFile);// 50581
        clear(outputFile);

        top100WithAtomicIntegerConcurrentHashMapLambada(filePath, outputFile);// 50819


//        clear(outputFile);
//        top100WithSingleThread(filePath, outputFile);// 69119
//        clear(outputFile);
//
//        top100WithDoubleStream(filePath, outputFile);// 99913
//        clear(outputFile);
//
//        top100WithErrorConcurrentHashMapLambada(filePath, outputFile);//err
//        clear(outputFile);
    }

    private static void clear(String outputFile) {
        File file = new File(outputFile);
        if (file.exists()) {
            file.delete();
        }
    }

    private static void top100WithSingleThread(String filePath, String outputFile) throws IOException {
        long l = System.currentTimeMillis();
        HashMap<String, Integer> dmap = new HashMap<>();
        HashMap<String, Integer> umap = new HashMap<>();
        BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            Log log = toLog(line);
            dmap.put(log.getD(), dmap.getOrDefault(log.getD(), 0) + 1);
            umap.put(log.getU(), umap.getOrDefault(log.getU(), 0) + 1);
        }

        // 排序
        List<String> dlist = dmap.entrySet()
                                 .stream()
                                 .sorted((o1, o2) -> o2.getValue() - o1.getValue())
                                 .limit(100)
                                 .map(x -> x.getKey() + "," + x.getValue())
                                 .collect(Collectors.toList());

        List<String> ulist = umap.entrySet()
                                 .stream()
                                 .sorted((o1, o2) -> o2.getValue() - o1.getValue())
                                 .limit(100)
                                 .map(x -> x.getKey() + "," + x.getValue())
                                 .collect(Collectors.toList());

        output(dlist, ulist, outputFile);
        System.out.println(System.currentTimeMillis() - l);

    }


    private static void top100WithDoubleStream(String filePath, String output) throws IOException {
        long l = System.currentTimeMillis();

        List<String> dlist = new BufferedReader(new FileReader(filePath))
                .lines()
                .parallel()
                .map(Top100::toLog)
                .collect(getLogListCollector(Log::getD));

        List<String> ulist = new BufferedReader(new FileReader(filePath))
                .lines()
                .parallel()
                .map(Top100::toLog)
                .collect(getLogListCollector(Log::getU));

        output(dlist, ulist, output);
        System.out.println(System.currentTimeMillis() - l);
    }

    private static Collector<Log, ?, List<String>> getLogListCollector(Function<Log, String> fun) {
        return Collectors.collectingAndThen(Collectors.groupingBy(fun, reducing(0, e -> 1, Integer::sum)), map -> mapToTop100List(map));
    }

    private static void top100WithAtomicIntegerConcurrentHashMapLambada(String filePath, String output) throws IOException {
        long l = System.currentTimeMillis();
        ConcurrentHashMap<String, AtomicInteger> dmap = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, AtomicInteger> umap = new ConcurrentHashMap<>();
        new BufferedReader(new FileReader(filePath))
                .lines()
                .parallel()
                .map(Top100::toLog)
                .forEach(log -> {
                    AtomicInteger atomicInteger = dmap.putIfAbsent(log.getD(), new AtomicInteger(1));
                    if (atomicInteger != null) {
                        atomicInteger.incrementAndGet();
                    }

                    AtomicInteger atomicInteger1 = umap.putIfAbsent(log.getU(), new AtomicInteger(1));
                    if (atomicInteger1 != null) {
                        atomicInteger1.incrementAndGet();
                    }
                });


        output(atomicMapToTop100List(dmap), atomicMapToTop100List(umap), output);
        System.out.println(System.currentTimeMillis() - l);

    }

    private static void top100WithSynchronizedHashMapLambada(String filePath, String output) throws IOException {
        long l = System.currentTimeMillis();
        Map<String, Integer> dmap = new HashMap<>();
        Map<String, Integer> umap = new HashMap<>();
        new BufferedReader(new FileReader(filePath))
                .lines()
                .parallel()
                .map(Top100::toLog)
                .forEach(log -> {
                    synchronized (dmap) {
                        dmap.put(log.getD(), dmap.getOrDefault(log.getD(), 0) + 1);
                        umap.put(log.getU(), umap.getOrDefault(log.getU(), 0) + 1);
                    }
                });

        output(mapToTop100List(dmap), mapToTop100List(umap), output);
        System.out.println(System.currentTimeMillis() - l);

    }

    private static void top100WithErrorConcurrentHashMapLambada(String filePath, String output) throws IOException {
        long l = System.currentTimeMillis();
        ConcurrentHashMap<String, Integer> dmap = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Integer> umap = new ConcurrentHashMap<>();
        new BufferedReader(new FileReader(filePath))
                .lines()
                .parallel()
                .map(Top100::toLog)
                .forEach(log -> {
                    dmap.put(log.getD(), dmap.getOrDefault(log.getD(), 0) + 1);
                    umap.put(log.getU(), umap.getOrDefault(log.getU(), 0) + 1);
                });


        output(mapToTop100List(dmap), mapToTop100List(umap), output);
        System.out.println(System.currentTimeMillis() - l);

    }

    private static List<String> mapToTop100List(Map<String, Integer> map) {
        return map.entrySet()
                  .stream()
                  .sorted((o1, o2) -> o2.getValue() - o1.getValue())
                  .limit(100)
                  .map(x -> x.getKey() + "," + x.getValue())
                  .collect(Collectors.toList());
    }

    private static List<String> atomicMapToTop100List(Map<String, AtomicInteger> map) {
        return map.entrySet()
                  .stream()
                  .sorted((o1, o2) -> o2.getValue().get() - o1.getValue().get())
                  .limit(100)
                  .map(x -> x.getKey() + "," + x.getValue())
                  .collect(Collectors.toList());
    }

    private static void output(List<String> dlist, List<String> ulist, String outputFile) throws IOException {

        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputFile));

        for (String s : dlist) {
            bufferedWriter.append(s).append(System.lineSeparator());
        }
        for (String s : ulist) {
            bufferedWriter.append(s).append(System.lineSeparator());
        }

        bufferedWriter.flush();
        bufferedWriter.close();
    }

    public static Log toLog(String line) {
        String[] split = line.split("\t")[3].split(" ");
        Log log = new Log();
        log.d = split[0];
        log.u = split[5].split("\\?")[0];
        return log;
    }


    static class Log {

        public String d;

        public String u;

        public String getD() {
            return d;
        }

        public void setD(String d) {
            this.d = d;
        }

        public String getU() {
            return u;
        }

        public void setU(String u) {
            this.u = u;
        }

        @Override
        public String toString() {
            return "Log{" +
                    "d='" + d + '\'' +
                    ", u='" + u + '\'' +
                    '}';
        }
    }


}
