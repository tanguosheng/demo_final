package singlesavememory.single;

public class SingleSaveMemeryEngine {

    public static void main(String[] args) {
        long begin = System.currentTimeMillis();
        String outPutPath = "/Users/apple/Downloads/";
        String domainFileName = "domain_" + begin;
        String urlFileName = "url_" + begin;
        String filePath = "/Users/apple/Downloads/access_20190926.log";
        SingleThreadReader.launch(filePath, outPutPath, domainFileName, urlFileName,
                10, 5, 19, 32);
        System.out.println(System.currentTimeMillis() - begin);
    }

}
