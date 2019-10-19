package single;

import java.io.File;

public class SingleEngine {

    public static void main(String[] args) {
        long begin = System.currentTimeMillis();
        String outPutPath = "/Users/apple/Downloads/";
        String domainFileName = "domain_" + begin;
        String urlFileName = "url_" + begin;

        File file = new File("/Users/apple/Downloads/access_20190926.log");
        // buffer size 16    18 最快
        SingleFileReader.launch(file, outPutPath, domainFileName, urlFileName,
                4, 19);
        System.out.println(System.currentTimeMillis() - begin);
    }

}
