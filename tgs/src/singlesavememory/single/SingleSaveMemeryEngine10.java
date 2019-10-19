package singlesavememory.single;

public class SingleSaveMemeryEngine10 {

    public static void main(String[] args) {
        long begin = System.currentTimeMillis();
        String outPutPath = "/Users/apple/Downloads/";
        String domainFileName = "domain_" + begin;
        String urlFileName = "url_" + begin;
        String filePath = "/Users/apple/Downloads/access_20190926_2.log";
        SingleThreadReader.launch(filePath, outPutPath, domainFileName, urlFileName,
                10, 6, 19, 32);
        System.out.println(System.currentTimeMillis() - begin);
    }

}
