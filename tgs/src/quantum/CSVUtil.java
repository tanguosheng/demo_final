package quantum;

import common.ArrayWrapper;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CSV文件导出工具类
 */

public class CSVUtil {

    /**
     * CSV文件生成方法
     */
    public static File createCSVFile(Map<String, AtomicInteger> head,
                                     List<Map.Entry<ArrayWrapper, AtomicInteger>> dataList,
                                     String outPutPath, String filename, int topNum) {

        File csvFile = null;
        BufferedWriter csvWtriter = null;
        try {
            csvFile = new File(outPutPath + File.separator + filename + ".csv");
            File parent = csvFile.getParentFile();
            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }

            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }
            csvFile.createNewFile();

            // GB2312使正确读取分隔符","
            csvWtriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
                    csvFile), "GB2312"), 1024);
//            // 写入文件头部
//            writeRowHead(head, csvWtriter);

            // 写入文件内容
            writeRow(dataList, csvWtriter, topNum);
            csvWtriter.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (csvWtriter != null) {
                    csvWtriter.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return csvFile;
    }

    /**
     * 写一行数据方法=
     */
    private static void writeRow(List<Map.Entry<ArrayWrapper, AtomicInteger>> row, BufferedWriter csvWriter,
                                 int topNum) throws IOException {
        if (row == null || row.size() == 0) {
            return;
        }
        if (topNum < 0) {
            topNum = row.size();
        }
        for (int i = 0; i < topNum; i++) {
            Map.Entry<ArrayWrapper, AtomicInteger> item = row.get(i);
            StringBuffer sb = new StringBuffer();
            String rowStr = sb.append("\"").append(item.getKey().toString()).append("\",").append(item.getValue().get())
                    .toString();
            csvWriter.write(rowStr);
            csvWriter.newLine();
        }
    }

//    private static void writeRowHead(Map<String, Integer> row, BufferedWriter csvWriter) throws IOException {
//        for (Map.Entry<String, Integer> data : row.entrySet()) {
//            StringBuffer sb = new StringBuffer();
//            String rowStr = sb.append("\"").append(data.getKey()).append("\",").toString();
//            csvWriter.write(rowStr);
//        }
//        csvWriter.newLine();
//    }
}

