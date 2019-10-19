package zzh;

import java.nio.MappedByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MapBuffReaderTask implements Runnable {

    private MappedByteBuffer mappedByteBuffer;

    private ConcurrentHashMap<Bytes, AtomicInteger> domainMap;

    private ConcurrentHashMap<Bytes, AtomicInteger> apiMap;

    private AtomicInteger signal;

    MapBuffReaderTask(MappedByteBuffer mappedByteBuffer, ConcurrentHashMap<Bytes, AtomicInteger> domainMap, ConcurrentHashMap<Bytes, AtomicInteger> apiMap,
            AtomicInteger signal) {
        this.mappedByteBuffer = mappedByteBuffer;
        this.domainMap = domainMap;
        this.apiMap = apiMap;
        this.signal = signal;
    }

    @Override
    public void run() {
        int bz = 1024 * 64;
        byte[] buff = new byte[bz];
        // 上次剩余
        byte[] left = new byte[0];

        try {
            for (;;) {
                mappedByteBuffer.get(buff);
                for (int index = bz - 1; index >= 0; index--) {
                    // 截取直到最后一个换行
                    if (buff[index] == '\n') {
                        byte[] submitBuff = new byte[left.length + index + 1];
                        if (left.length != 0) {
                            System.arraycopy(left, 0, submitBuff, 0, left.length);
                        }
                        System.arraycopy(buff, 0, submitBuff, left.length, index + 1);
                        processLine(submitBuff);
                        
                        // 拷贝剩余字符到字节组
                        int leftLen = bz - index - 1;
                        left = new byte[leftLen];
                        if (leftLen != 0) {
                            System.arraycopy(buff, index + 1, left, 0, leftLen);
                        }
                        break;
                    }
                }
            }
        } catch (Exception e) {
            buff = new byte[mappedByteBuffer.remaining()];
            mappedByteBuffer.get(buff);
            byte[] submitBuff = new byte[left.length + buff.length];
            if (left.length != 0) {
                System.arraycopy(left, 0, submitBuff, 0, left.length);
            }
            System.arraycopy(buff, 0, submitBuff, left.length, buff.length);
            processLine(submitBuff);
        } finally {
            signal.decrementAndGet();
        }
    }

    private void processLine(byte[] buff) {
        try {
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
                                AtomicInteger domainInteger = domainMap.computeIfAbsent(key, k -> new AtomicInteger(0));
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
                } catch (Exception e) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
