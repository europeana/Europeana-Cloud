package eu.europeana.cloud.helpers;

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
    static final String WRITE_RECORD_BOLT = "writeRecordBolt";
    static final String DATASET_NAME = "dataSet";
    static final String DATA_PROVIDER = "testDataProvider";

    static final String SOURCE_VERSION_URL = "http://localhost:8080/mcs/records/"
            + SOURCE + CLOUD_ID + "/representations/"
            + SOURCE + REPRESENTATION_NAME + "/versions/"
            + SOURCE + VERSION + "/files/"
            + SOURCE + FILE;
    static final String RESULT_VERSION_URL = "http://localhost:8080/mcs/records/"
            + RESULT + CLOUD_ID + "/representations/"
            + RESULT + REPRESENTATION_NAME + "/versions/"
            + RESULT + VERSION;
    static final String RESULT_FILE_URL = RESULT_VERSION_URL + "/files/" + FILE;

    static final String SOURCE_DATASET_URL = "http://localhost:8080/mcs/data-providers/"
            + DATA_PROVIDER + "/data-sets/"
            + DATASET_NAME;


    static final int NUM_WORKERS = 2;

    //topology elements Strings
    static final String SPOUT = "spout";
    static final String PARSE_TASK_BOLT = "parseTaskBolt";
    static final String RETRIEVE_FILE_BOLT = "retrieveFileBolt";
    static final String IC_BOLT = "icBolt";
    static final String GRANT_PERMISSIONS_TO_FILE_BOLT = "grantPermissionsToFileBolt";
    static final String REMOVE_PERMISSIONS_TO_FILE_BOLT = "removePermissionsToFileBolt";
    static final String END_BOLT = "endBolt";
    static final String NOTIFICATION_BOLT = "notificationBolt";
    static final String TEST_END_BOLT = "testEndBolt";
    static final List<String> PRINT_ORDER = Arrays.asList(SPOUT, PARSE_TASK_BOLT, RETRIEVE_FILE_BOLT, IC_BOLT, WRITE_RECORD_BOLT, GRANT_PERMISSIONS_TO_FILE_BOLT, REMOVE_PERMISSIONS_TO_FILE_BOLT, END_BOLT, NOTIFICATION_BOLT, TEST_END_BOLT);

}
