package eu.europeana.cloud.service.dps.examples.util;

import java.util.List;

import com.google.common.collect.Lists;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;

/**
 * dps Task helpers
 *
 * @author manos
 */
public class DpsTaskUtil {

    // number of records to be included
    private final static int RECORD_COUNT = 1;

    // Some Crappy hardcoded string for testing
    private final static String FILE_URL = "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/"
            + "L9WSPSMVQ85/representations/edm/versions/b17c4f60-70d0-11e4-8fe1-00163eefc9c8/files/ef9322a1-5416-4109-a727-2bdfecbf352d";

    // This is some real xml from TEL
    private final static String XML_URL = "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/L9WSPSMVQ85/"
            + "representations/edm/versions/b17c4f60-70d0-11e4-8fe1-00163eefc9c8/files/977cdca3-cd69-44cd-819f-1e5fbfe610e3";

    // sample XML
    private final static String SAMPLE_XML = "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/"
            + "L9WSPSMVQ85/representations/edm/versions/b17c4f60-70d0-11e4-8fe1-00163eefc9c8/files/af7d3a77-4b00-485f-832c-a33c5a3d7b56";

    // OUTPUT_URL
    private final static String OUTPUT_URL = "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/L9WSPSMVQ85/"
            + "representations/edm/versions/b17c4f60-70d0-11e4-8fe1-00163eefc9c8";

    // INPUT URL hosted in polish server
    private final static String POZNAN_INPUT_URL = "http://felicia.man.poznan.pl/mcs/records/FJ0DL14CDQB/representations/"
            + "oai-pmh/versions/c68eccb0-b7ad-11e4-93c4-00505682006e/files/46b0ab4b-78f2-45e3-aa97-658efff60e19";

    // INPUT URL hosted in polish server (manos)
    private final static String POZNAN_INPUT_URL_MANOS = "http://felicia.man.poznan.pl/mcs/records/M9TXDFG2CXT/representations/edm/versions/"
            + "fd5d7060-c1b8-11e4-8d39-00505682006e/files/e3f8b355-c079-45bc-ab31-b4b3ccf8f536";

    // xslt hosted in ISTI (Franco Maria)
    private final static String xsltUrl = "http://pomino.isti.cnr.it/~nardini/eCloudTest/a0429_xslt";

    // xslt hosted in ULCC
    private final static String ulccXsltUrl = "http://ecloud.eanadev.org:8080/hera/sample_xslt.xslt";


    private final static String SAMPLE_TIFF = "http://iks-kbase.synat.pcss.pl:9090/mcs/records/JP46FLZLVI2UYV4JNHTPPAB4DGPESPY4SY4N5IUQK4SFWMQ3NUQQ/representations/tiff/versions/74c56880-7733-11e5-b38f-525400ea6731/files/f59753a5-6d75-4d48-9f4d-4690b671240c";
    /**
     * @return a hardcoded {@link DpsTask}
     */
    public static DpsTask generateDpsTask() {
        return generateDpsTask(SAMPLE_XML, ulccXsltUrl, RECORD_COUNT);
    }

    public static DpsTask generateDpsTask(final String xmlUrl, final String xslt, final int recordCount) {

        DpsTask task = new DpsTask();

        List<String> records = Lists.newArrayList();
        for (int i = 0; i < recordCount; i++) {

            // adding the input files!
            records.add(xmlUrl);
        }

        task.addDataEntry(DpsTask.FILE_URLS, records);
        task.addParameter(PluginParameterKeys.XSLT_URL, xslt);
        task.addParameter(PluginParameterKeys.OUTPUT_URL, null);

        return task;
    }

    public static DpsTask generateDpsTaskForIc(final String imageUrl, final int recordCount) {

        DpsTask task = new DpsTask();

        List<String> records = Lists.newArrayList();
        for (int i = 0; i < recordCount; i++) {
            records.add(imageUrl);
        }

        task.addDataEntry(DpsTask.FILE_URLS, records);
        task.addParameter(PluginParameterKeys.NEW_REPRESENTATION_NAME, "jp2reprooo");
        task.addParameter(PluginParameterKeys.OUTPUT_MIME_TYPE,"image/jp2");
        //task.addParameter(PluginParameterKeys.INPUT_EXTENSION, "tiff");
        ///task.addParameter(PluginParameterKeys.OUTPUT_EXTENSION, "jp2");
        //  task.setTaskName("nameOfTheTask");
        // task.addParameter("outPut");

        return task;
    }

    public static DpsTask generateDPsTaskForIC() {
        return generateDpsTaskForIc(SAMPLE_TIFF, RECORD_COUNT);
    }


}
