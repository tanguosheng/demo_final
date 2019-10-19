package quantum;


public class QuantumEngine {

    public static void main(String[] args) {
        long begin = System.currentTimeMillis();
        String filePath = "/Users/apple/Downloads/access_20190926.log";
        String outPutPath = "/Users/apple/Downloads/";
        String domainFileName = "domain" + begin;
        String urlFileName = "uri" + begin;
        QuantumFileReader.launch(filePath, outPutPath, domainFileName, urlFileName, 4, 27);
        System.out.println(System.currentTimeMillis() - begin);
    }

}
