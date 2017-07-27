package eu.europeana.cloud.service.dps.test;

/**
 * Created by Tarek on 3/13/2017.
 */
public class TestConstants {
    //test Strings
    public static final String RESULT = "result";
    public static final String SOURCE = "source";
    public static final String CLOUD_ID = "CloudId";
    public static final String LOCAL_ID = "LOCAL_ID";
    public static final String CLOUD_ID2 = "CloudId2";
    public static final String REPRESENTATION_NAME = "RepresentationName";
    public static final String VERSION = "Version";
    public static final String REVISIONS = "revisions";
    public static final String FILE = "FileName";
    public static final String DATASET_NAME = "dataSet";
    public static final String DATA_PROVIDER = "testDataProvider";

    public static final String SOURCE_VERSION_URL = "http://localhost:8080/mcs/records/"
            + SOURCE + CLOUD_ID + "/representations/"
            + SOURCE + REPRESENTATION_NAME + "/versions/"
            + SOURCE + VERSION + "/files/"
            + SOURCE + FILE;
    public static final String SOURCE_VERSION_URL2 = "http://localhost:8080/mcs/records/"
            + SOURCE + CLOUD_ID + "/representations/"
            + SOURCE + REPRESENTATION_NAME + "/versions/"
            + SOURCE + VERSION + 2 + "/files/"
            + SOURCE + FILE;
    public static final String RESULT_VERSION_URL = "http://localhost:8080/mcs/records/"
            + RESULT + CLOUD_ID + "/representations/"
            + RESULT + REPRESENTATION_NAME + "/versions/"
            + RESULT + VERSION;


    public static final String REVISION_URL = "http://localhost:8080/mcs/records/"
            + RESULT + CLOUD_ID + "/representations/"
            + RESULT + REPRESENTATION_NAME + "/versions/"
            + RESULT + REVISIONS;

    public static final String RESULT_FILE_URL = RESULT_VERSION_URL + "/files/" + FILE;

    public static final String SOURCE_DATASET_URL = "http://localhost:8080/mcs/data-providers/"
            + DATA_PROVIDER + "/data-sets/"
            + DATASET_NAME;
    public static final String SOURCE_DATASET_URL2 = SOURCE_DATASET_URL + 2;
    public static final String MCS_URL = "http://localhost:8080/mcs";
    public static final String UIS_URL = "http://localhost:8080/uis";

    public static final int NUM_WORKERS = 1;


    public static final String TASK_NAME = "TASK_NAME";
    public static final String REVISION_NAME = "REVISION_NAME";
    public static final String REVISION_PROVIDER = "REVISION_PROVIDER";

    public static final String TEST_END_BOLT = "testEndBolt";

}
