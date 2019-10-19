package common;

import quantum.CSVUtil;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class AnalysisHandle {

    /**
     * \n
     */
    private static final byte NEW_LINE_SYMBOL = 10;

    public static void analysis(byte[] readBuff, int length,
                                Map<ArrayWrapper, AtomicInteger> domainMap,
                                Map<ArrayWrapper, AtomicInteger> uriMap) {
        try {
            int index = 0;
            for (; ; ) {
                try {
//                    if (index >= length) {
//                        break;
//                    }
                    // 跳过前三个table
                    for (; ; ) {
                        byte b = readBuff[index++];
                        if (b == '\t') {
                            break;
                        }
                    }

                    for (; ; ) {
                        byte b = readBuff[index++];
                        if (b == '\t') {
                            break;
                        }
                    }

                    for (; ; ) {
                        byte b = readBuff[index++];
                        if (b == '\t') {
                            break;
                        }
                    }

                    int domainStart = index;
                    int apiStart = 0;
                    int spCn = 0;

                    // 读取内容
                    for (; ; ) {
                        byte b = readBuff[index++];
                        if (b == '\t') {
                            break;
                        }

                        if (b == 32) {
                            spCn++;
                            if (spCn == 1) {
                                increment(readBuff, index, domainStart, domainMap);
                            } else if (spCn == 5) {
                                apiStart = index;
                            } else if (spCn == 6) {
                                increment(readBuff, index, apiStart, uriMap);
                                break;
                            }
                        }

                        if (spCn == 5 && b == 63) {
                            increment(readBuff, index, apiStart, uriMap);
                            break;
                        }
                    }

                    // 读到行尾部
                    for (; ; ) {
                        byte b = readBuff[index++];
                        if (b == NEW_LINE_SYMBOL) {
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

    private static void increment(byte[] readBuff, int index, int domainStart, Map<ArrayWrapper, AtomicInteger> map) {
        int len = index - 1 - domainStart;
        byte[] domainBuff = new byte[len];
        System.arraycopy(readBuff, domainStart, domainBuff, 0, len);
        ArrayWrapper key = new ArrayWrapper(domainBuff);
        AtomicInteger atomicInteger = map
                .computeIfAbsent(key, k -> new AtomicInteger(0));
        atomicInteger.incrementAndGet();
    }

    public static void mergeResultAndPostHandle(Collection<Map<ArrayWrapper, AtomicInteger>> resultList,
                                                String outPutPath,
                                                String fileName) {
        Map<ArrayWrapper, AtomicInteger> map = AnalysisHandle.mergeResult(resultList);
        postHandle(map, outPutPath, fileName);
    }

    private static Map<ArrayWrapper, AtomicInteger> mergeResult(
            Collection<Map<ArrayWrapper, AtomicInteger>> resultList) {
        Map<ArrayWrapper, AtomicInteger> map = new HashMap<>();
        for (Map<ArrayWrapper, AtomicInteger> item : resultList) {
            for (Map.Entry<ArrayWrapper, AtomicInteger> entry : item.entrySet()) {
                AtomicInteger atomicInteger = map.get(entry.getKey());
                if (atomicInteger == null) {
                    map.put(entry.getKey(), entry.getValue());
                } else {
                    atomicInteger.addAndGet(entry.getValue().get());
                }
            }
        }
        return map;
    }

    public static void postHandle(Map<ArrayWrapper, AtomicInteger> map, String outPutPath, String fileName) {
        List<Map.Entry<ArrayWrapper, AtomicInteger>> domainList = new ArrayList<>(map.entrySet());
        domainList.sort((e1, e2) -> -Integer.compare(e1.getValue().get(), e2.getValue().get()));

        CSVUtil.createCSVFile(null, domainList, outPutPath, fileName, 100);
    }


}
