package zzh;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ByteBuffReader {

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        try {
            String outputPath = "/Users/apple/Downloads/";
            String name = "zzh";
            String domainFileName = name + "_domain_my";
            String urlFileName = name + "_uri_my";
            int poolSize = 4;

            // 线程池以及统计map
            LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
            ConcurrentHashMap<Bytes, AtomicInteger> domainMap = new ConcurrentHashMap<>();
            ConcurrentHashMap<Bytes, AtomicInteger> apiMap = new ConcurrentHashMap<>();
            ThreadPoolExecutor pool = new ThreadPoolExecutor(poolSize, poolSize, 10, TimeUnit.SECONDS, workQueue, new CustomThreadFactory());

            // 批次大小
            int bz = 1024 * 512;
            // 剩余字节缓存大小
            int leftBuffSize = 1024 * 8;
            // 缓存大小
            int buffSize = bz + leftBuffSize;

            // 缓存
            int cacheSize = 32;
            LinkedBlockingQueue<ByteBuffer> emptyQueue = new LinkedBlockingQueue<>(cacheSize);
            for (int i = 0; i < cacheSize; i++) {
                ByteBuffer buff = ByteBuffer.allocate(buffSize);
                emptyQueue.offer(buff);
            }

            // 打开流
            FileInputStream s = new FileInputStream("/Users/apple/Downloads/access_20190926_2.log");
            BufferedInputStream fis = new BufferedInputStream(s, bz);
            byte[] buff = new byte[bz];

            // 上次剩余
            ByteBuffer left = ByteBuffer.allocate(leftBuffSize);
            for (;;) {
                int len = fis.read(buff);
                ByteBuffer byteBuff = emptyQueue.take();
                for (int index = len - 1; index >= 0; index--) {
                    // 截取直到最后一个换行
                    if (buff[index] == '\n') {
                        int leftLen = left.position();
                        if (leftLen != 0) {
                            byteBuff.put(left.array(), 0, leftLen);
                        }
                        byteBuff.put(buff, 0, index + 1);
                        pool.execute(new ByteBuffReaderTask(byteBuff, domainMap, apiMap, emptyQueue));
                        left.clear();

                        // 拷贝剩余字符到字节组
                        leftLen = len - index - 1;
                        if (leftLen != 0) {
                            left.put(buff, index + 1, leftLen);
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
