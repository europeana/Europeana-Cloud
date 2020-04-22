package eu.europeana.cloud.mcs.driver;

import eu.europeana.cloud.common.model.Representation;
import org.junit.Ignore;
import org.junit.Test;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static eu.europeana.cloud.common.web.ParamConstants.*;

//@Ignore
public class RecordServiceClientTestIT {
    private  static final String TEST_URL = "http://localhost:8080/mcs";

    private static final String SUPER_USERNAME = "metis_test";  //user z bazy danych
    private static final String SUPER_PASSWORD = "1RkZBuVf";
    private static final String PROVIDER_ID = "Arek-TEST-02";
    private static final String DATA_SET_ID = "DATA_SET_ID";

    private static final String C_ID_0 = "BZCLXTC4P4CABCYZKUPHS5M5ZZTWAJEXBW7JUIVMMENXT6UWZPKA";
    private static final String REPRESENTATION_NAME = "TESTOW_REPREZENTACJA_01";
    public static final String VERSION_0 = "e63f5070-b834-11e9-8559-024299ecb71b";

    @Test
    public void createRepresentation() {
        try {
            RecordServiceClient mcsClient = new RecordServiceClient(TEST_URL, SUPER_USERNAME, SUPER_PASSWORD);
            URI uri = mcsClient.createRepresentation(C_ID_0, REPRESENTATION_NAME, PROVIDER_ID);
            System.out.println(uri);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//http://localhost:8080/mcs/records/2YRDQTLJMPCN264Y75CVIJ65RPZP5DJFS36CYAGMNIGT3GLKLMDA/representations/edm/versions
//https://test.ecloud.psnc.pl/api/records/2YRDQTLJMPCN264Y75CVIJ65RPZP5DJFS36CYAGMNIGT3GLKLMDA/representations/edm/versions

//https://test.ecloud.psnc.pl/api/records/SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA/representations/metadataRecord/versions/
//http://localhost:8080/mcs/records/SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA/representations/metadataRecord/versions/

    @Test
    public void getRepresentations() {
        try {
            RecordServiceClient mcsClient = new RecordServiceClient(TEST_URL, null, null);
            List<Representation> result = mcsClient.getRepresentations("SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA", "metadataRecord");
            //System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

/*
    @Test
    public void testReplace() {
        MappingPlaceholdersResolver r = new MappingPlaceholdersResolver();

        final String CLASS_MAPPING =
                "/records/{"+CLOUD_ID+"}/representations/{"+REPRESENTATION_NAME+"}/versions/{"+VERSION+"}/files/{"+FILE_NAME+":(.+)?}";

        Properties properties = new Properties();
        properties.setProperty(CLOUD_ID, "CLOUD_ID");
        properties.setProperty(REPRESENTATION_NAME, "REPRESENTATION_NAME");
        properties.setProperty(VERSION, "VERSION");
        properties.setProperty(FILE_NAME, "FILE_NAME");

        String s = r.replacePlaceholders(CLASS_MAPPING, properties);
        System.err.println(s);
    }

    public static class MappingPlaceholdersResolver {
        private static final char OPEN = '{';
        private static final char CLOSE = '}';
        private static final char REG_EXP_SEPRATOR = ':';

        public String replacePlaceholders(String path, Properties properties) {
            StringBuilder result = new StringBuilder();

            int currentIndex = 0;
            int prevIndex = 0;

            while(currentIndex != -1) {
                currentIndex = path.indexOf(OPEN, prevIndex);
                if(currentIndex != -1) {
                    result.append(path.substring(prevIndex, currentIndex));

                    int closeIndex = path.indexOf(CLOSE, currentIndex);
                    if(closeIndex == -1) {
                        result.append(path.substring(prevIndex));
                    } else {
                        prevIndex = processPlaceholder(path.substring(currentIndex, closeIndex+1), properties, closeIndex, result);
                    }
                } else {
                    result.append(path.substring(prevIndex));
                }
            }
            return result.toString();
        }

        private int processPlaceholder(String placeHolder, Properties properties, int closeIndex, StringBuilder result) {
            int regExpSepLocalIndex = placeHolder.indexOf(REG_EXP_SEPRATOR);

            String placeholderName = null;
            String placeholderRegExp = null;

            if(regExpSepLocalIndex == -1) {
                placeholderName = placeHolder.substring(1, placeHolder.length()-1).trim();
            } else {
                placeholderName = placeHolder.substring(1, regExpSepLocalIndex).trim();
                placeholderRegExp = placeHolder.substring(regExpSepLocalIndex+1, placeHolder.length()-1).trim();
            }

            if(!properties.containsKey(placeholderName)) {
                result.append(placeHolder);
            } else {
                String value = properties.getProperty(placeholderName);

                if(placeholderRegExp == null || value.matches(placeholderRegExp)) {
                    result.append(value);
                } else {
                    result.append(placeHolder);
                }
            }

            return closeIndex+1;
        }


    }
*/

}
