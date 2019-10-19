package zzh;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class ByteBuffReaderTask implements Runnable {

    private ByteBuffer byteBuffer;

    private ConcurrentHashMap<Bytes, AtomicInteger> stMap;

    private ConcurrentHashMap<Bytes, AtomicInteger> apiMap;

    private LinkedBlockingQueue<ByteBuffer> emptyQueue;

    ByteBuffReaderTask(ByteBuffer byteBuffer, ConcurrentHashMap<Bytes, AtomicInteger> stMap, ConcurrentHashMap<Bytes, AtomicInteger> apiMap,
            LinkedBlockingQueue<ByteBuffer> emptyQueue) {
        this.byteBuffer = byteBuffer;
        this.stMap = stMap;
        this.apiMap = apiMap;
        this.emptyQueue = emptyQueue;
    }

    @Override
    public void run() {
        try {
            byte[] buff = byteBuffer.array();
            int index = 0;
            for (;;) {
                try {
                    // 跳过前三个table
                    for (;;) {
                        byte b = buff[index++];
                        if (b == '\t') {
                            break;
                        }
                    }

                    for (;;) {
                        byte b = buff[index++];
                        if (b == '\t') {
                            break;
                        }
                    }

                    for (;;) {
                        byte b = buff[index++];
                        if (b == '\t') {
                            break;
                        }
                    }

                    int domainStart = index;
                    int apiStart = 0;
                    int spCn = 0;

                    // 读取内容
                    for (;;) {
                        byte b = buff[index++];
                        if (b == '\t') {
                            break;
                        }

                        if (b == 32) {
                            spCn++;
                            if (spCn == 1) {
                                int len = index - 1 - domainStart;
                                byte[] domainBuff = new byte[len];
                                System.arraycopy(buff, domainStart, domainBuff, 0, len);
                                Bytes key = new Bytes(domainBuff);
                                AtomicInteger domainInteger = stMap.computeIfAbsent(key, k -> new AtomicInteger(0));
                                domainInteger.incrementAndGet();
                            } else if (spCn == 5) {
                                apiStart = index;
                            } else if (spCn == 6) {
                                int len = index - 1 - apiStart;
                                byte[] apiBuff = new byte[len];
                                System.arraycopy(buff, apiStart, apiBuff, 0, len);
                                Bytes key = new Bytes(apiBuff);
                                AtomicInteger domainInteger = apiMap.computeIfAbsent(key, k -> new AtomicInteger(0));
                                domainInteger.incrementAndGet();
                                break;
                            }
                        }

                        if (spCn == 5 && b == 63) {
                            int len = index - 1 - apiStart;
                            byte[] apiBuff = new byte[len];
                            System.arraycopy(buff, apiStart, apiBuff, 0, len);
                            Bytes key = new Bytes(apiBuff);
                            AtomicInteger domainInteger = apiMap.computeIfAbsent(key, k -> new AtomicInteger(0));
                            domainInteger.incrementAndGet();
                            break;
                        }
                    }

                    // 读到行尾部
                    for (;;) {
                        byte b = buff[index++];
                        if (b == '\n') {
                            break;
                        }
                    }

                    // 跳过有效内容
                    if (index >= byteBuffer.position()) {
                        break;
                    }
                } catch (Exception e) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 清楚标记，并重新加入空队列
            byteBuffer.clear();
            emptyQueue.offer(byteBuffer);
        }
    }
}
