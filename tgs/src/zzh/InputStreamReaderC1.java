package zzh;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class InputStreamReaderC1 {

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        try {
            String outputPath = "/Users/mac/Desktop/st/";
            String name = "zzh";
            String domainFileName = name + "_domain_my";
            String urlFileName = name + "_uri_my";
            int poolSize = 4;

            LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
            ConcurrentHashMap<Bytes, AtomicInteger> domainMap = new ConcurrentHashMap<>();
            ConcurrentHashMap<Bytes, AtomicInteger> apiMap = new ConcurrentHashMap<>();

            // 异步拆分字节
            LinkedBlockingQueue<byte[]> cacheQueue = new LinkedBlockingQueue<>(1000);

            FileInputStream s = new FileInputStream("/Users/mac/Desktop/st/access_all.log");
            int bz = 1024 * 512;

            BufferedInputStream fis = new BufferedInputStream(s, bz);
            byte[] buff = new byte[bz];

            // 结束标记
            final AtomicBoolean start = new AtomicBoolean(true);

            // Thread t = new Thread(new Runnable() {
            // @Override
            // public void run() {
            // byte[] left = new byte[0];
            // while (start.get()) {
            // while (!cacheQueue.isEmpty()) {
            // byte[] buff = cacheQueue.poll();
            // int len = buff.length;
            // for (int index = len - 1; index >= 0; index--) {
            // // 截取直到最后一个换行
            // if (buff[index] == '\n') {
            // byte[] submitBuff = new byte[left.length + index + 1];
            // if (left.length != 0) {
            // System.arraycopy(left, 0, submitBuff, 0, left.length);
            // }
            // System.arraycopy(buff, 0, submitBuff, left.length, index + 1);
            // pool.execute(new InputStreamReaderTask(submitBuff, domainMap, apiMap));
            //
            // // 拷贝剩余字符到字节组
            // int leftLen = len - index - 1;
            // left = new byte[leftLen];
            // if (leftLen != 0) {
            // System.arraycopy(buff, index + 1, left, 0, leftLen);
            // }
            // break;
            // }
            // }
            // }
            // }
            // }
            // });
            // t.start();

            long startT = System.currentTimeMillis();
            for (;;) {
                int len = fis.read(buff);
                if (len < bz) {
                    byte[] temp = new byte[len];
                    System.arraycopy(buff, 0, temp, 0, len);
                    // cacheQueue.add(temp);
                    start.set(false);
                    break;
                }
                // cacheQueue.add(buff);
                buff = new byte[bz];
            }
            System.out.println("read content use = " + (System.currentTimeMillis() - startT));
            // t.join();

            while (workQueue.size() != 0) {
                Thread.sleep(1);
            }

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
