package eu.europeana.cloud.mcs.driver;


import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class DataSetServiceClientTestIT {

  private static final String LOCAL_TEST_URL = "http://localhost:8080/mcs";

  private static final String USER_NAME = "metis_test";  //user z bazy danych
  private static final String USER_PASSWORD = "Gi*Z26h4c1y^rTGf";

  private static final Logger LOGGER = LoggerFactory.getLogger(DataSetServiceClientTestIT.class);

  @Test
  void createDataSet() throws MCSException {
    String providerId = "<enter_provider_id_here>";
    String dataSetId = "<enter_data_set_id_here>";
    String description = "<enter_description_here_can_be_null>";

    DataSetServiceClient mcsClient = new DataSetServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);

    URI dataSetURI = mcsClient.createDataSet(providerId, dataSetId, description);

    assertNotNull(dataSetURI);
  }

  @Test
  void getDataSetRepresentationsChunk() throws MCSException {
    String providerId = "<enter_provider_id_here>";
    String dataSetId = "<enter_data_set_id_here>";
    String startFrom = "<enter_start_from_here>";

    DataSetServiceClient mcsClient = new DataSetServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);

    ResultSlice<Representation> response = mcsClient.getDataSetRepresentationsChunk(providerId, dataSetId, false, startFrom);

    assertNotNull(response);
    assertNotNull(response.getResults());
  }


  @Test
  void getDataSetRepresentationsChunkRealData() throws MCSException {
    String providerId = "metis_acceptance";
    String dataSetId = "218068ec-aad2-4bd3-9421-9bfcefe92e2a";
    String startFrom = null;

    DataSetServiceClient mcsClient = new DataSetServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);

    ResultSlice<Representation> response = mcsClient.getDataSetRepresentationsChunk(providerId, dataSetId, false, startFrom);

    assertNotNull(response);
    assertNotNull(response.getResults());
  }

  @Test
  void getDataSetsForProviderChunk() throws MCSException {
    String providerId = "<enter_provider_id_here>";
    String startFrom = "<enter_start_from_here>";

    DataSetServiceClient mcsClient = new DataSetServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);

    ResultSlice<DataSet> response = mcsClient.getDataSetsForProviderChunk(providerId, startFrom);

    assertNotNull(response);
    assertNotNull(response.getResults());
  }

  @Test
  void getDataSetsForProviderChunkRealData() throws MCSException {
    String providerId = "metis_acceptance";
    String startFrom = null;

    DataSetServiceClient mcsClient = new DataSetServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);

    ResultSlice<DataSet> response = mcsClient.getDataSetsForProviderChunk(providerId, startFrom);

    assertNotNull(response);
    assertNotNull(response.getResults());
  }


  @Test
  void testGetDataSetsForProvider() throws MCSException {
    String providerId = "metis_acceptance";
    DataSetServiceClient mcsClient = new DataSetServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);

    List<DataSet> response = mcsClient.getDataSetsForProviderList(providerId);

    assertNotNull(response);

  }

  @Test
  void testGetDataSetIteratorForProvider() {
    String providerId = "metis_acceptance";
    DataSetServiceClient mcsClient = new DataSetServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);

    DataSetIterator it = mcsClient.getDataSetIteratorForProvider(providerId);

    while (it.hasNext()) {
      assertNotNull(it.next());
    }
  }

  @Test
  void testGetDataSetRepresentations() throws MCSException {
    String providerId = "metis_acceptance";
    String dataSetId = "6f193618-476a-4431-a78a-69571df58163";
    DataSetServiceClient mcsClient = new DataSetServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);

    ResultSlice<Representation> result = mcsClient.getDataSetRepresentations(providerId, dataSetId, false);
    assertNotNull(result);
  }

}
