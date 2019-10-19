package zzh;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MapBuffReader {

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        try {
            String outputPath = "/Users/mac/Desktop/st/";
            String name = "zzh";
            String domainFileName = name + "_domain_my";
            String urlFileName = name + "_uri_my";
            int poolSize = 12;

            LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
            ConcurrentHashMap<Bytes, AtomicInteger> domainMap = new ConcurrentHashMap<>();
            ConcurrentHashMap<Bytes, AtomicInteger> apiMap = new ConcurrentHashMap<>();
            ThreadPoolExecutor pool = new ThreadPoolExecutor(poolSize, poolSize, 10, TimeUnit.SECONDS, workQueue, new CustomThreadFactory());

            // 打开文件
            RandomAccessFile file = new RandomAccessFile("/Users/mac/Desktop/st/access_20190926.log", "r");
            FileChannel fc = file.getChannel();

            // 文件总长度
            long len = file.length();

            // 批次大小
            long stepSize = 1024 * 1024 * 128;

            // 拆分文件，保证完整行
            List<MapLineInfo> splitList = new ArrayList<>();
            byte[] temp = new byte[1024];
            // 结束坐标
            long endIndex = 0;
            
            // 拆分开始时间
            long splitStart = System.currentTimeMillis();
            while (endIndex < len) {
                long startIndex = endIndex;
                endIndex = startIndex + stepSize;
                int offset = 0;
                if (endIndex >= len) {
                    // 结束
                    endIndex = len;
                }
                file.seek(endIndex);

                // 往后读1024个字节
                while (file.read(temp) == 1024) {
                    boolean finish = false;
                    for (byte b : temp) {
                        if (b == '\n') {
                            finish = true;
                            break;
                        }
                        offset++;
                    }
                    if (finish) {
                        break;
                    }
                }

                MapLineInfo line = new MapLineInfo();
                endIndex = endIndex + offset;
                line.setStart(startIndex);
                line.setSize(endIndex - startIndex);
                splitList.add(line);
            }
            System.out.println("split use time =: " + (System.currentTimeMillis() - splitStart));

            // 线程池执行完毕信号
            AtomicInteger signal = new AtomicInteger(splitList.size());

            // 提交多线程处理每部分内容
            for (MapLineInfo lineInfo : splitList) {
                MappedByteBuffer mappedByteBuffer = fc.map(FileChannel.MapMode.READ_ONLY, lineInfo.getStart(), lineInfo.getSize());
                pool.execute(new MapBuffReaderTask(mappedByteBuffer, domainMap, apiMap, signal));
            }

            // 等待线程池执行完毕

            while (signal.get() != 0) {
                Thread.sleep(1);
            }

            // 关闭线程池
            pool.shutdown();

            // 处理文件导出
            ArrayList<Map.Entry<Bytes, AtomicInteger>> results = sortMap(domainMap);
            CSVUtil.createCSVFile(null, results, outputPath, domainFileName, 100);
            results = sortMap(apiMap);
            CSVUtil.createCSVFile(null, results, outputPath, urlFileName, 100);

            // 关闭文件流
            file.close();
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
