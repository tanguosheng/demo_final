package quantum;

public class QuantumEngine10 {

    public static void main(String[] args) {
        long begin = System.currentTimeMillis();
        String filePath = "/Users/apple/Downloads/access_20190926_2.log";
        String outPutPath = "/Users/apple/Downloads/";
        String domainFileName = "domain" + begin;
        String urlFileName = "uri" + begin;

        QuantumFileReader.launch(filePath, outPutPath, domainFileName, urlFileName, 8, 27);
        System.out.println(System.currentTimeMillis() - begin);
    }


}
