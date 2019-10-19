package single;


import java.io.File;

public class SingleEngine10 {

    public static void main(String[] args) {
        long begin = System.currentTimeMillis();
        String outPutPath = "/Users/apple/Downloads/";
        String domainFileName = "domain_" + begin;
        String urlFileName = "url_" + begin;

        File file = new File("/Users/apple/Downloads/access_10g.log");
        // buffer size 16    18 最快
        SingleFileReader.launch(file, outPutPath, domainFileName, urlFileName, 4, 19);
        System.out.println(System.currentTimeMillis() - begin);
    }

}
