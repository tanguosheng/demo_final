package zzh;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class InputStreamReaderTask implements Runnable {

    private byte[] buff;

    private ConcurrentHashMap<Bytes, AtomicInteger> stMap;

    private ConcurrentHashMap<Bytes, AtomicInteger> apiMap;

    InputStreamReaderTask(byte[] buff, ConcurrentHashMap<Bytes, AtomicInteger> stMap, ConcurrentHashMap<Bytes, AtomicInteger> apiMap) {
        this.buff = buff;
        this.stMap = stMap;
        this.apiMap = apiMap;
    }

    @Override
    public void run() {
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
                } catch (Exception e) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
