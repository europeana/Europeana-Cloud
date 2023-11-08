package eu.europeana.cloud.service.dps.test;

/**
 * Created by Tarek on 3/13/2017.
 */
public final class TestConstants {

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
  public static final String FILE2 = "FileName2";
  public static final String DATASET_NAME = "dataSet";
  public static final String DATA_PROVIDER = "testDataProvider";

  public static final String MCS_URL = "http://localhost:8080/mcs";

  public static final String RECORDS_URI_PART = "/records/";
  public static final String REPRESENTATIONS_URI_PART = "/representations/";
  public static final String VERSIONS_URI_PART = "/versions/";
  public static final String FILES_URI_PART = "/files/";
  public static final String DATA_SETS_URI_PART = "/data-sets/";
  public static final String DATA_PROVIDERS_URI_PART = "/data-providers/";
  public static final String SOURCE_VERSION_URL = MCS_URL + RECORDS_URI_PART
      + SOURCE + CLOUD_ID + REPRESENTATIONS_URI_PART
      + SOURCE + REPRESENTATION_NAME + VERSIONS_URI_PART
      + SOURCE + VERSION + FILES_URI_PART
      + SOURCE + FILE;

  public static final String SOURCE_VERSION_URL_CLOUD_ID2 = MCS_URL + RECORDS_URI_PART
      + SOURCE + CLOUD_ID2 + REPRESENTATIONS_URI_PART
      + SOURCE + REPRESENTATION_NAME + VERSIONS_URI_PART
      + SOURCE + VERSION + FILES_URI_PART
      + SOURCE + FILE;


  public static final String SOURCE_VERSION_URL_FILE2 = MCS_URL + RECORDS_URI_PART
      + SOURCE + CLOUD_ID + REPRESENTATIONS_URI_PART
      + SOURCE + REPRESENTATION_NAME + VERSIONS_URI_PART
      + SOURCE + VERSION + FILES_URI_PART
      + SOURCE + FILE2;

  public static final String SOURCE_VERSION_URL2 = MCS_URL + RECORDS_URI_PART
      + SOURCE + CLOUD_ID + REPRESENTATIONS_URI_PART
      + SOURCE + REPRESENTATION_NAME + VERSIONS_URI_PART
      + SOURCE + VERSION + 2 + FILES_URI_PART
      + SOURCE + FILE;
  public static final String RESULT_VERSION_URL = MCS_URL + RECORDS_URI_PART
      + RESULT + CLOUD_ID + REPRESENTATIONS_URI_PART
      + RESULT + REPRESENTATION_NAME + VERSIONS_URI_PART
      + RESULT + VERSION;
  public static final String RESULT_VERSION_URL2 = MCS_URL + RECORDS_URI_PART
      + RESULT + CLOUD_ID + REPRESENTATIONS_URI_PART
      + RESULT + REPRESENTATION_NAME + VERSIONS_URI_PART
      + RESULT + VERSION + 2;


  public static final String REVISION_URL = MCS_URL + RECORDS_URI_PART
      + RESULT + CLOUD_ID + REPRESENTATIONS_URI_PART
      + RESULT + REPRESENTATION_NAME + VERSIONS_URI_PART
      + RESULT + REVISIONS;

  public static final String RESULT_FILE_URL = RESULT_VERSION_URL + FILES_URI_PART + FILE;
  public static final String RESULT_FILE_URL2 = RESULT_VERSION_URL2 + FILES_URI_PART + FILE2;


  public static final String SOURCE_DATASET_URL = MCS_URL + DATA_PROVIDERS_URI_PART
      + DATA_PROVIDER + DATA_SETS_URI_PART
      + DATASET_NAME;
  public static final String SOURCE_DATASET_URL2 = SOURCE_DATASET_URL + 2;
  public static final String UIS_URL = "http://localhost:8080/uis";

  public static final int NUM_WORKERS = 1;


  public static final String TASK_NAME = "TASK_NAME";
  public static final String REVISION_NAME = "REVISION_NAME";
  public static final String REVISION_PROVIDER = "REVISION_PROVIDER";

  public static final String TEST_END_BOLT = "testEndBolt";

  private TestConstants() {
  }
}
