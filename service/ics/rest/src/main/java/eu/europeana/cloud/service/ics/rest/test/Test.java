package eu.europeana.cloud.service.ics.rest.test;

import eu.europeana.cloud.service.ics.rest.data.FileInputParameter;
import eu.europeana.cloud.service.ics.rest.rest_java_client.ICSServiceClient;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by Tarek on 8/17/2015.
 */
public class Test {
    private static String cloudId = "TAJZ2ZVNTXLQ6R5SMBY2MYKONUCFBFMPY2TCQNMA2ZL4CXHMATHA";
    private static String inputFileExtension = "tiff";
    private static String inputRepresentation = "represent_befor_push";
    private static String outputFileExtension = "jp2";
    private static String outputRepresentation = "represent_befor_push_output";
    private static String provider_id = "Tiff_tarek_final";
    private static String inputVersion = "6d96d640-52dd-11e5-8242-1c6f653f9042";
    private static String fileName = "430b2f34-a4f9-4e5c-91f9-64b51ee75d88";
    private static String url = "http://localhost:8080/ics/converter/json/single/file";

    public static void main(String[] args){
        try {
            FileInputParameter inputParameter = constructInputParameter();
            ICSServiceClient client = new ICSServiceClient(url);
            System.out.println(client.convertFile(inputParameter));
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

    }

    public static FileInputParameter constructInputParameter() {
        FileInputParameter inputParameter = new FileInputParameter();
        inputParameter.setCloudId(cloudId);
        inputParameter.setInputExtension(inputFileExtension);
        inputParameter.setInputRepresentationName(inputRepresentation);
        inputParameter.setOutputExtension(outputFileExtension);
        inputParameter.setOutputRepresentationName(outputRepresentation);
        inputParameter.setProviderId(provider_id);
        inputParameter.setInputVersion(inputVersion);
        inputParameter.setFileName(fileName);
        List<String> properties = new ArrayList<String>();
        properties.add("-rate 0.00009");
        inputParameter.setProperties(properties);
        return inputParameter;
    }
}
