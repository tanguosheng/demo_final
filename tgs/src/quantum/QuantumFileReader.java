package quantum;

import common.AnalysisHandle;
import common.ArrayWrapper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class QuantumFileReader {
    private String outPutPath;
    private String domainFilename;
    private String uriFilename;
    private int pairSize;
    private int bufferSize;
    private ExecutorService readExecutorService;
    private long fileLength;
    private RandomAccessFile rAccessFile;
    private Set<StartEndPair> startEndPairs;
    private Collection<Map<ArrayWrapper, AtomicInteger>> domainResultList;
    private Collection<Map<ArrayWrapper, AtomicInteger>> uriResultList;

    private static final ThreadLocal<Map<ArrayWrapper, AtomicInteger>> domainThreadLocal = new ThreadLocal<>();
    private static final ThreadLocal<Map<ArrayWrapper, AtomicInteger>> uriThreadLocal = new ThreadLocal<>();
    private static final ThreadLocal<byte[]> byteBufferThreadLocal = new ThreadLocal<>();

    private Map<ArrayWrapper, AtomicInteger> domainC = new ConcurrentHashMap<>();
    private Map<ArrayWrapper, AtomicInteger> uriC = new ConcurrentHashMap<>();

    public QuantumFileReader(File file, String outPutPath, String domainFilename, String uriFilename,
                             int pairSize, int readThreadPoolSize) {
        this.outPutPath = outPutPath;
        this.domainFilename = domainFilename;
        this.uriFilename = uriFilename;
        this.fileLength = file.length();
        this.pairSize = pairSize;
        this.domainResultList = new CopyOnWriteArrayList<>();
        this.uriResultList = new CopyOnWriteArrayList<>();

        try {
            this.rAccessFile = new RandomAccessFile(file, "r");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        this.readExecutorService = Executors.newFixedThreadPool(readThreadPoolSize);
        startEndPairs = new HashSet<>((int) (fileLength / pairSize));
    }

    public void start() {
        try {
            long startTime = System.currentTimeMillis();
            bufferSize = (int) calculateStartEnd(pairSize, fileLength);
            System.out.println("calculateStartEnd use: " + (System.currentTimeMillis() - startTime));

            FileChannel fc = rAccessFile.getChannel();

            // 将分片丢进线程池处理
            for (StartEndPair pair : startEndPairs) {
                MappedByteBuffer mappedByteBuffer = fc
                        .map(FileChannel.MapMode.READ_ONLY, pair.start, pair.length);
                readExecutorService.execute(new SliceReaderTask(pair.length, mappedByteBuffer));
            }

            // 安全关闭线程池
            readExecutorService.shutdown();
            // 等待积压在线程池队列内的任务结束
            while (!readExecutorService.isTerminated()) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            this.rAccessFile.close();

            AnalysisHandle.mergeResultAndPostHandle(domainResultList, outPutPath, domainFilename);
            AnalysisHandle.mergeResultAndPostHandle(uriResultList, outPutPath, uriFilename);

//            AnalysisHandle.postHandle(domainC, outPutPath, domainFilename);
//            AnalysisHandle.postHandle(uriC, outPutPath, uriFilename);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private class SliceReaderTask implements Runnable {
        private int length;
        MappedByteBuffer mapBuffer;

        public SliceReaderTask(int length, MappedByteBuffer mapBuffer) {
            this.mapBuffer = mapBuffer;
            this.length = length;
        }

        private byte[] readBuff;
        private Map<ArrayWrapper, AtomicInteger> domainMap;
        private Map<ArrayWrapper, AtomicInteger> uriMap;

        private void prepare() {
            domainMap = domainThreadLocal.get();
            uriMap = uriThreadLocal.get();
            readBuff = byteBufferThreadLocal.get();
            if (domainMap == null) {
                domainMap = new HashMap<>();
                domainThreadLocal.set(domainMap);
                domainResultList.add(domainMap);
            }
            if (uriMap == null) {
                uriMap = new HashMap<>();
                uriThreadLocal.set(uriMap);
                uriResultList.add(uriMap);
            }
            if (readBuff == null) {
                readBuff = new byte[bufferSize];
                byteBufferThreadLocal.set(readBuff);
            }
        }

        @Override
        public void run() {
            prepare();
            mapBuffer.get(readBuff, 0, length);
            AnalysisHandle.analysis(readBuff, length, domainMap, uriMap);
//            AnalysisHandle.analysis(readBuff, length, domainC, uriC);
        }
    }

    private long calculateStartEnd(long pairSize, long fileLength) throws IOException {
        long start = 0;
        long maxLength = 0;
        while (true) {
            long endPosition = start + pairSize - 1;
            if (start > fileLength - 1) {
                return maxLength;
            }
            StartEndPair pair = new StartEndPair();
            pair.start = start;
            if (endPosition >= fileLength - 1) {
                pair.length = (int) (fileLength - pair.start);
                startEndPairs.add(pair);
                if (pair.length > maxLength) {
                    maxLength = pair.length;
                }
                return maxLength;
            }
            rAccessFile.seek(endPosition);
            byte tmp = rAccessFile.readByte();
            while (tmp != '\n' && tmp != '\r') {
                endPosition++;
                if (endPosition >= fileLength - 1) {
                    endPosition = fileLength - 1;
                    break;
                }
                rAccessFile.seek(endPosition);
                tmp = rAccessFile.readByte();
            }
            pair.length = (int) (endPosition - pair.start + 1);
            startEndPairs.add(pair);
            if (pair.length > maxLength) {
                maxLength = pair.length;
            }
            start = endPosition + 1;
        }
    }

    private static class StartEndPair {
        private long start;
        private int length;
    }

    public static class Builder {
        private String outPutPath;
        private String domainFilename;
        private String uriFilename;
        private int readThreadPoolSize = 1;
        private int pairSize = Integer.MAX_VALUE;
        private File file;

        public Builder(File file, String outPutPath, String domainFilename, String uriFilename) {
            this.file = file;
            this.outPutPath = outPutPath;
            this.domainFilename = domainFilename;
            this.uriFilename = uriFilename;
            if (!this.file.exists()) {
                throw new IllegalArgumentException("文件不存在！");
            }
        }

        public QuantumFileReader.Builder readThreadPoolSize(int size) {
            this.readThreadPoolSize = size;
            return this;
        }

        public QuantumFileReader.Builder pairSize(int pairSize) {
            this.pairSize = pairSize;
            return this;
        }

        public QuantumFileReader build() {
            return new QuantumFileReader(this.file, this.outPutPath, this.domainFilename, this.uriFilename,
                    this.pairSize, this.readThreadPoolSize);
        }
    }

    public static void launch(String filePath, String outPutPath, String domainFileName, String urlFileName,
                               int threadPoolSize, int pairSizePower) {
        QuantumFileReader.Builder builder = new QuantumFileReader.Builder(new File(filePath), outPutPath,
                domainFileName,
                urlFileName);
        QuantumFileReader handle = builder
                .readThreadPoolSize(threadPoolSize)
                .pairSize(1 << pairSizePower)
                .build();
        handle.start();
    }
}
