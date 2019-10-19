package zzh;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Statistics {

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        try {
            String outputPath = "/Users/mac/Desktop/st/";
            String name = "zzh";
            String domainFileName = name + "_domain_my";
            String urlFileName = name + "_uri_my";

            LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
            int poolSize = 4;

            // ConcurrentLinkedQueue<Map<String, AtomicInteger>> domainQueue = new ConcurrentLinkedQueue<>();
            // ConcurrentLinkedQueue<Map<String, AtomicInteger>> apiQueue = new ConcurrentLinkedQueue<>();
            // ConcurrentHashMap<String, AtomicInteger> stMap = new ConcurrentHashMap<>();
            //
            // for (int i = 0; i < poolSize; i++) {
            // domainQueue.add(new HashMap<String, AtomicInteger>());
            // apiQueue.add(new HashMap<String, AtomicInteger>());
            // }

            ConcurrentHashMap<String, AtomicInteger> domainMap = new ConcurrentHashMap<>();
            ConcurrentHashMap<String, AtomicInteger> apiMap = new ConcurrentHashMap<>();

            ThreadPoolExecutor pool = new ThreadPoolExecutor(poolSize, poolSize, 10, TimeUnit.SECONDS, workQueue, new CustomThreadFactory());

            FileInputStream s = new FileInputStream("/Users/mac/Desktop/st/access_20190926.log");
            BufferedInputStream fis = new BufferedInputStream(s, 8192);

            // int bz = 524288; // 7078
            // int bz = 65536; // 7401
            int bz = 8192; // 7224
            byte[] buff = new byte[bz];
            // 上次剩余
            byte[] left = new byte[0];
            for (;;) {
                int len = fis.read(buff);
                for (int index = len - 1; index >= 0; index--) {
                    // 截取直到最后一个换行
                    if (buff[index] == '\n') {
                        byte[] submitBuff = new byte[left.length + index + 1];
                        if (left.length != 0) {
                            System.arraycopy(left, 0, submitBuff, 0, left.length);
                        }
                        System.arraycopy(buff, 0, submitBuff, left.length, index + 1);
                        pool.execute(new InputStreamTask(submitBuff, domainMap, apiMap));

                        // 拷贝剩余字符到字节组
                        int leftLen = len - index - 1;
                        left = new byte[leftLen];
                        if (leftLen != 0) {
                            System.arraycopy(buff, index + 1, left, 0, leftLen);
                        }
                        break;
                    }
                }

                if (len < bz) {
                    break;
                }
            }

            while (workQueue.size() != 0) {
                Thread.sleep(1);
            }
            pool.shutdown();

            ArrayList<Map.Entry<String, AtomicInteger>> results = sortMap(domainMap);
            CSVUtil2.createCSVFile(null, results, outputPath, domainFileName, 100);
            results = sortMap(apiMap);
            CSVUtil2.createCSVFile(null, results, outputPath, urlFileName, 100);

            fis.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("use time =: " + (System.currentTimeMillis() - startTime));
    }

    private static ArrayList<Map.Entry<String, AtomicInteger>> sortMap(Map<String, AtomicInteger> map) {
        ArrayList<Map.Entry<String, AtomicInteger>> list = new ArrayList<>(map.entrySet());
        list.sort((e1, e2) -> {
            int one = e1.getValue().get();
            int two = e2.getValue().get();
            return -Integer.compare(one, two);
        });
        return list;
    }
}
