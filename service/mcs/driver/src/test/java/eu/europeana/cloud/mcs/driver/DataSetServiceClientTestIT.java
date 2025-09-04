package eu.europeana.cloud.mcs.driver;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import java.net.URI;
import java.util.List;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSetServiceClientTestIT {

  private static final String LOCAL_TEST_URL = "http://localhost:8080/mcs";

  private static final String USER_NAME = "metis_test";  //user z bazy danych
  private static final String USER_PASSWORD = "Gi*Z26h4c1y^rTGf";

  private static final Logger LOGGER = LoggerFactory.getLogger(DataSetServiceClientTestIT.class);

  @Test
  public void createDataSet() throws MCSException {
    String providerId = "<enter_provider_id_here>";
    String dataSetId = "<enter_data_set_id_here>";
    String description = "<enter_description_here_can_be_null>";

    DataSetServiceClient mcsClient = new DataSetServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);

    URI dataSetURI = mcsClient.createDataSet(providerId, dataSetId, description);

    assertNotEquals(dataSetURI, null);
  }

  @Test
  public void getDataSetRevisionsChunk() throws MCSException {
    String providerId = "<enter_provider_id_here>";
    String dataSetId = "<enter_data_set_id_here>";
    String representationName = "<enter_representation_name_here>";
    String revisionName = "<enter_revision_name_here>";
    String revisionProviderId = "<enter_revision_provider_id_here>";
    String revisionTimestamp = "<enter_revision_timestamp_here[YYYY-MM-ddThh:mm:ss.sss]>";
    String startFrom = "<enter_start_from_here_can_be_null>";
    Integer limit = 0;

    DataSetServiceClient mcsClient = new DataSetServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);

    ResultSlice<CloudTagsResponse> response =
        mcsClient.getDataSetRevisionsChunk(providerId, dataSetId, representationName,
            new Revision(revisionName, revisionProviderId, DateHelper.parseISODate(revisionTimestamp)),
            startFrom, limit);

    assertNotNull(response);
    assertNotNull(response.getResults());
  }

  //https://test.ecloud.psnc.pl/api/data-providers/metis_acceptance/data-sets/218068ec-aad2-4bd3-9421-9bfcefe92e2a/representations/metadataRecord/revisions/VALIDATION_EXTERNAL/revisionProvider/metis_acceptance?revisionTimestamp=2019-09-26T16:30:04.972
  @Test
  public void getDataSetRevisionsChunkRealData() throws MCSException {
    String providerId = "metis_acceptance";
    String dataSetId = "218068ec-aad2-4bd3-9421-9bfcefe92e2a";
    String representationName = "metadataRecord";
    String revisionName = "VALIDATION_EXTERNAL";
    String revisionProviderId = "metis_acceptance";
    String revisionTimestamp = "2019-09-26T16:30:04.972";
    String startFrom = null;

    DataSetServiceClient mcsClient = new DataSetServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);

    ResultSlice<CloudTagsResponse> response =
        mcsClient.getDataSetRevisionsChunk(providerId, dataSetId, representationName,
            new Revision(revisionName, revisionProviderId, DateHelper.parseISODate(revisionTimestamp)),
            startFrom, null);

    assertNotNull(response);
    assertNotNull(response.getResults());
  }

  @Test
  public void getRevisionsWithDeletedFlagSetToFalse() throws MCSException {
    String providerId = "xxx";
    String dataSetId = "autotests";
    String representationName = "xxx";
    String revisionName = "OAIPMH_HARVEST";
    String revisionProviderId = "xxx";
    String revisionTimestamp = "2021-09-22T06:45:02.592";
    int limit = 1000;

    DataSetServiceClient mcsClient = new DataSetServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);

    List<CloudTagsResponse> response = mcsClient.getRevisionsWithDeletedFlagSetToFalse(providerId, dataSetId,
        representationName, revisionName, revisionProviderId, revisionTimestamp, limit);

    assertNotNull(response);
    LOGGER.info(response.toString());
  }

  @Test
  public void getDataSetRepresentationsChunk() throws MCSException {
    String providerId = "<enter_provider_id_here>";
    String dataSetId = "<enter_data_set_id_here>";
    String startFrom = "<enter_start_from_here>";

    DataSetServiceClient mcsClient = new DataSetServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);

    ResultSlice<Representation> response = mcsClient.getDataSetRepresentationsChunk(providerId, dataSetId, false, startFrom);

    assertNotNull(response);
    assertNotNull(response.getResults());
  }


  @Test
  public void getDataSetRepresentationsChunkRealData() throws MCSException {
    String providerId = "metis_acceptance";
    String dataSetId = "218068ec-aad2-4bd3-9421-9bfcefe92e2a";
    String startFrom = null;

    DataSetServiceClient mcsClient = new DataSetServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);

    ResultSlice<Representation> response = mcsClient.getDataSetRepresentationsChunk(providerId, dataSetId, false, startFrom);

    assertNotNull(response);
    assertNotNull(response.getResults());
  }

  @Test
  public void getDataSetsForProviderChunk() throws MCSException {
    String providerId = "<enter_provider_id_here>";
    String startFrom = "<enter_start_from_here>";

    DataSetServiceClient mcsClient = new DataSetServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);

    ResultSlice<DataSet> response = mcsClient.getDataSetsForProviderChunk(providerId, startFrom);

    assertNotNull(response);
    assertNotNull(response.getResults());
  }

  @Test
  public void getDataSetsForProviderChunkRealData() throws MCSException {
    String providerId = "metis_acceptance";
    String startFrom = null;

    DataSetServiceClient mcsClient = new DataSetServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);

    ResultSlice<DataSet> response = mcsClient.getDataSetsForProviderChunk(providerId, startFrom);

    assertNotNull(response);
    assertNotNull(response.getResults());
  }


  @Test
  public void testGetDataSetsForProvider() throws MCSException {
    String providerId = "metis_acceptance";
    DataSetServiceClient mcsClient = new DataSetServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);

    List<DataSet> response = mcsClient.getDataSetsForProviderList(providerId);

    assertNotNull(response);

  }

  @Test
  public void testGetDataSetIteratorForProvider() {
    String providerId = "metis_acceptance";
    DataSetServiceClient mcsClient = new DataSetServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);

    DataSetIterator it = mcsClient.getDataSetIteratorForProvider(providerId);

    while (it.hasNext()) {
      assertNotNull(it.next());
    }
  }

  @Test
  public void testGetDataSetRepresentations() throws MCSException {
    String providerId = "metis_acceptance";
    String dataSetId = "6f193618-476a-4431-a78a-69571df58163";
    DataSetServiceClient mcsClient = new DataSetServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);

    ResultSlice<Representation> result = mcsClient.getDataSetRepresentations(providerId, dataSetId, false);
    assertNotNull(result);
  }

  @Test
  public void testGetDataSetRevisions() throws MCSException {
    String providerId = "metis_acceptance";
    String dataSetId = "6f193618-476a-4431-a78a-69571df58163";
    String representationName = "metadataRecord";
    String revisionName = "TRANSFORMATION";
    String revisionProviderId = "metis_acceptance";
    String revisionTimestamp = "2019-11-22T13:50:34.413Z";
    DataSetServiceClient mcsClient = new DataSetServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);

    List<CloudTagsResponse> result = mcsClient.getDataSetRevisionsList(providerId, dataSetId, representationName,
        new Revision(revisionName, revisionProviderId, DateHelper.parseISODate(revisionTimestamp)));

    assertNotNull(result);
  }
}
