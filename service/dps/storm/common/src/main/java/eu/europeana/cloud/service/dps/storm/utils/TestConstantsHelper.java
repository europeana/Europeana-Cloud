package eu.europeana.cloud.service.dps.storm.utils;

import java.util.Arrays;
import java.util.List;

/**
 * @author akrystian.
 */
public interface TestConstantsHelper {
    //test Strings
    static final String RESULT = "result";
    static final String SOURCE = "source";
    static final String CLOUD_ID = "CloudId";
    static final String REPRESENTATION_NAME = "RepresentationName";
    static final String VERSION = "Version";
    static final String FILE = "FileName";
    static final String DATASET_NAME = "dataSet";
    static final String DATA_PROVIDER = "testDataProvider";

    static final String SOURCE_VERSION_URL = "http://localhost:8080/mcs/records/"
            + SOURCE + CLOUD_ID + "/representations/"
            + SOURCE + REPRESENTATION_NAME + "/versions/"
            + SOURCE + VERSION + "/files/"
            + SOURCE + FILE;
    static final String SOURCE_VERSION_URL2 = "http://localhost:8080/mcs/records/"
            + SOURCE + CLOUD_ID + "/representations/"
            + SOURCE + REPRESENTATION_NAME + "/versions/"
            + SOURCE + VERSION + 2 + "/files/"
            + SOURCE + FILE;
    static final String RESULT_VERSION_URL = "http://localhost:8080/mcs/records/"
            + RESULT + CLOUD_ID + "/representations/"
            + RESULT + REPRESENTATION_NAME + "/versions/"
            + RESULT + VERSION;
    static final String RESULT_FILE_URL = RESULT_VERSION_URL + "/files/" + FILE;

    static final String SOURCE_DATASET_URL = "http://localhost:8080/mcs/data-providers/"
            + DATA_PROVIDER + "/data-sets/"
            + DATASET_NAME;
    static final String SOURCE_DATASET_URL2 = SOURCE_DATASET_URL + 2;


    static final int NUM_WORKERS = 2;


    static final String TEST_END_BOLT = "testEndBolt";
    static final List<String> PRINT_ORDER = Arrays.asList(TopologyHelper.SPOUT, TopologyHelper.PARSE_TASK_BOLT, TopologyHelper.READ_DATASETS_BOLT, TopologyHelper.READ_DATASET_BOLT, TopologyHelper.READ_REPRESENTATION_BOLT, TopologyHelper.RETRIEVE_FILE_BOLT, TopologyHelper.IC_BOLT, TopologyHelper.WRITE_RECORD_BOLT, TopologyHelper.NOTIFICATION_BOLT, TEST_END_BOLT);

}
