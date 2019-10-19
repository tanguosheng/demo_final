package zzh;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class InputStreamReader {

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        try {
            String outputPath = "/Users/apple/Downloads/";
            String name = "zzh";
            String domainFileName = name + "_domain_my";
            String urlFileName = name + "_uri_my";
            int poolSize = 4;

            LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
            ConcurrentHashMap<Bytes, AtomicInteger> domainMap = new ConcurrentHashMap<>();
            ConcurrentHashMap<Bytes, AtomicInteger> apiMap = new ConcurrentHashMap<>();
            ThreadPoolExecutor pool = new ThreadPoolExecutor(poolSize, poolSize, 10, TimeUnit.SECONDS, workQueue, new CustomThreadFactory());

            FileInputStream s = new FileInputStream("/Users/apple/Downloads/access_20190926_2.log");
            int bz = 1024 * 512;

            BufferedInputStream fis = new BufferedInputStream(s, bz);
            byte[] buff = new byte[bz];

            // 上次剩余
            LeftByteBuffer left = new LeftByteBuffer(1024 * 8);
            for (;;) {
                int len = fis.read(buff);
                for (int index = len - 1; index >= 0; index--) {
                    // 截取直到最后一个换行
                    if (buff[index] == '\n') {
                        byte[] submitBuff = new byte[left.size() + index + 1];
                        if (left.size() != 0) {
                            left.copy(submitBuff, 0);
                        }
                        System.arraycopy(buff, 0, submitBuff, left.size(), index + 1);
                        pool.execute(new InputStreamReaderTask(submitBuff, domainMap, apiMap));
                        left.reset();

                        // 拷贝剩余字符到字节组
                        int leftLen = len - index - 1;
                        if (leftLen != 0) {
                            left.appendArray(buff, index + 1, leftLen);
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

            ArrayList<Map.Entry<Bytes, AtomicInteger>> results = sortMap(domainMap);
            CSVUtil.createCSVFile(null, results, outputPath, domainFileName, 100);
            results = sortMap(apiMap);
            CSVUtil.createCSVFile(null, results, outputPath, urlFileName, 100);

            fis.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("use time =: " + (System.currentTimeMillis() - startTime));
    }

    private static ArrayList<Map.Entry<Bytes, AtomicInteger>> sortMap(Map<Bytes, AtomicInteger> map) {
        ArrayList<Map.Entry<Bytes, AtomicInteger>> list = new ArrayList<>(map.entrySet());
        list.sort((e1, e2) -> {
            int one = e1.getValue().get();
            int two = e2.getValue().get();
            return -Integer.compare(one, two);
        });
        return list;
    }
}
