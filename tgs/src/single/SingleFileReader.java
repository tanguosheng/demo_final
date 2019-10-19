package single;

import common.AnalysisHandle;
import common.ArrayWrapper;
import common.Buffer;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SingleFileReader {
    private File file;
    private int bufferSize;
    private String domainFilename;
    private String uriFilename;
    private ExecutorService executorService;
    private LinkedBlockingQueue<String> pipeline;
    private String outPutPath;
    private List<Map<ArrayWrapper, AtomicInteger>> domainResultList;
    private List<Map<ArrayWrapper, AtomicInteger>> uriResultList;
    private static final ThreadLocal<Map<ArrayWrapper, AtomicInteger>> domainThreadLocal = new ThreadLocal<>();
    private static final ThreadLocal<Map<ArrayWrapper, AtomicInteger>> uriThreadLocal = new ThreadLocal<>();

    private Map<ArrayWrapper, AtomicInteger> domainC = new ConcurrentHashMap<>();
    private Map<ArrayWrapper, AtomicInteger> uriC = new ConcurrentHashMap<>();

    public SingleFileReader(File file, int bufferSize, int threadPoolSize,
                            String outPutPath, String domainFilename, String uriFilename) {
        this.file = file;
        this.outPutPath = outPutPath;
        this.domainFilename = domainFilename;
        this.uriFilename = uriFilename;
        this.bufferSize = bufferSize;
        this.executorService = Executors.newFixedThreadPool(threadPoolSize);
        this.domainResultList = new CopyOnWriteArrayList<>();
        this.uriResultList = new CopyOnWriteArrayList<>();
    }

    public void start() {
        try {
//            readByBuffer();
            read();

            // 安全关闭线程池
            executorService.shutdown();
            // 等待积压在线程池队列内的任务结束
            while (!executorService.isTerminated()) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            AnalysisHandle.mergeResultAndPostHandle(domainResultList, outPutPath, domainFilename);
            AnalysisHandle.mergeResultAndPostHandle(uriResultList, outPutPath, uriFilename);

//            AnalysisHandle.postHandle(domainC, outPutPath, domainFilename);
//            AnalysisHandle.postHandle(uriC, outPutPath, uriFilename);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void readByBuffer() throws Exception {
        byte[] byteBuffer = new byte[bufferSize];
        int cnt = 0;
        int offset = 0;
        int length = bufferSize;

        FileInputStream fis = new FileInputStream(this.file);
        BufferedInputStream bis = new BufferedInputStream(fis);

        while ((cnt = bis.read(byteBuffer, offset, length)) != -1) {
            int lastIndex = offset + cnt - 1;
            for (; lastIndex >= 0; lastIndex--) {
                if (byteBuffer[lastIndex] == '\n') {
                    break;
                }
            }
            if (lastIndex == -1) {
                executorService.shutdown();
                bis.close();
                throw new Exception("请加大buffer size.");
            }
            byte[] temp = new byte[lastIndex + 1];
            System.arraycopy(byteBuffer, 0, temp, 0, lastIndex + 1);
            executorService.submit(new SliceReaderTask(temp));
            System.arraycopy(byteBuffer, lastIndex + 1, byteBuffer, 0, bufferSize - lastIndex - 1);
            offset = bufferSize - lastIndex - 1;
            length = bufferSize - offset;
        }
        bis.close();
    }

    private void read() throws IOException {
        byte[] byteBuffer = new byte[bufferSize];
        int cnt = 0;
        int offset = 0;
        int length = bufferSize;

        FileInputStream fis = new FileInputStream(this.file);
        BufferedInputStream bis = new BufferedInputStream(fis);

        Buffer buffer = new Buffer(1 << 16);
        for (; ; ) {
            cnt = bis.read(byteBuffer, offset, length);
            if (cnt == -1) {
                break;
            }
            int lastIndex = cnt - 1;
            for (; lastIndex >= 0; lastIndex--) {
                if (byteBuffer[lastIndex] == '\n') {
                    break;
                }
            }
            if (lastIndex != -1) {
                byte[] temp = new byte[buffer.size() + lastIndex + 1];
                buffer.copy(temp, 0);
                System.arraycopy(byteBuffer, 0, temp, buffer.size(), lastIndex + 1);
                executorService.submit(new SliceReaderTask(temp));
                buffer.reset();
            }
            buffer.appendArray(byteBuffer, lastIndex + 1, cnt - 1 - lastIndex);
        }

        bis.close();
    }

    private class SliceReaderTask implements Runnable {
        byte[] bytes;

        public SliceReaderTask(byte[] bytes) {
            this.bytes = bytes;
        }

        private Map<ArrayWrapper, AtomicInteger> domainMap;
        private Map<ArrayWrapper, AtomicInteger> uriMap;

        private void prepare() {
            domainMap = domainThreadLocal.get();
            uriMap = uriThreadLocal.get();
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
        }

        @Override
        public void run() {
            prepare();
            AnalysisHandle.analysis(bytes, bytes.length, domainMap, uriMap);
//            AnalysisHandle.analysis(bytes, bytes.length, domainC, uriC);
        }
    }

    public static class Builder {
        private int bufferSize = 1024 * 1024;
        private int threadPoolSize = 2;
        private String outPutPath;
        private String domainFilename;
        private String uriFilename;
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

        public Builder threadPoolSize(int size) {
            this.threadPoolSize = size;
            return this;
        }

        public Builder bufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
            return this;
        }

        public SingleFileReader build() {
            return new SingleFileReader(this.file, this.bufferSize,
                    this.threadPoolSize, this.outPutPath, this.domainFilename, this.uriFilename);
        }
    }

    public static void launch(File file, String outPutPath, String domainFileName, String urlFileName,
                              int threadPoolSize, int bufferSizePower) {
        SingleFileReader.Builder builder = new SingleFileReader.Builder(file, outPutPath, domainFileName, urlFileName);
        SingleFileReader bigSingleFileReader = builder
                .threadPoolSize(threadPoolSize)
                .bufferSize(1 << bufferSizePower)
                .build();
        bigSingleFileReader.start();
    }
}

