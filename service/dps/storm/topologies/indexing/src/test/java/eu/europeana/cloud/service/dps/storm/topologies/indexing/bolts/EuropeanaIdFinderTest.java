package eu.europeana.cloud.service.dps.storm.topologies.indexing.bolts;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.dps.storm.TopologyGeneralException;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EuropeanaIdFinderTest {

  private static final String METIS_DATASET_ID = "11";
  private static final String CLOUD_ID = "CXJVA4V473SUOKUT2AX6ZAI6KKD6NDVY6GRALFVRIEVKOOBUZA6A";
  private static final String FILE_URL = "http://localhost:8080/mcs/records/CXJVA4V473SUOKUT2AX6ZAI6KKD6NDVY6GRALFVRIEVKOOBUZA6A/representations/test_representation/versions/b7620030-6b9e-11eb-a9d0-04922659f621";
  private static final String PROVIDER_ID = "test_provider";
  private static final String NOT_EUROPEANA_ID = "abcd";
  private static final String LOCAL_ID_1 = "http://localhost:8080/oai/abcd";
  private static final String EUROPEANA_ID_1 = "/11/abcd";
  private static final String EUROPEANA_ID_2 = "/11/_11_abcd";
  private static final String EUROPEANA_ID_3 = "/11/efgh";


  @Mock
  private UISClient uisClient;

  @Mock
  private HarvestedRecordsDAO harvestedRecordsDAO;

  @InjectMocks
  private EuropeanaIdFinder finder;

  private final List<CloudId> idsFromUIS = new ArrayList<>();
  private final ResultSlice<CloudId> resultSlice = new ResultSlice<>(null, idsFromUIS);

  @Before
  public void before() throws CloudException {
    when(uisClient.getRecordId(CLOUD_ID)).thenReturn(resultSlice);
    when(harvestedRecordsDAO.findRecord(anyString(), anyString())).thenReturn(Optional.empty());
  }

  @Test(expected = CloudException.class)
  public void shouldThrowExceptionWhenNotEuropeanaIdCouldBeFound() throws MalformedURLException, CloudException {
    when(uisClient.getRecordId(CLOUD_ID)).thenThrow(createRecordNotExistsException());

    finder.findForFileUrl(METIS_DATASET_ID, FILE_URL);
  }

  @Test
  public void shouldReturnAnyLocalIdIfItIsTheOnlyOne() throws MalformedURLException, CloudException {
    idsFromUIS.add(createId(NOT_EUROPEANA_ID));

    String europeanaId = finder.findForFileUrl(METIS_DATASET_ID, FILE_URL);

    assertEquals(NOT_EUROPEANA_ID, europeanaId);
  }

  @Test
  public void shouldReturnEuropeanaIdIfOtherIdsNotStartsWithMetisDatasetIdPrefix() throws MalformedURLException, CloudException {
    idsFromUIS.add(createId(LOCAL_ID_1));
    idsFromUIS.add(createId(EUROPEANA_ID_1));

    String europeanaId = finder.findForFileUrl(METIS_DATASET_ID, FILE_URL);

    assertEquals(EUROPEANA_ID_1, europeanaId);
  }


  @Test
  public void shouldReturnValidEuropeanaIdIfOtherIdStartsOddlyWithMetisDatasetIdPrefixButLocalIdPartIsContainedInIt()
      throws MalformedURLException, CloudException {
    idsFromUIS.add(createId(EUROPEANA_ID_2));
    idsFromUIS.add(createId(EUROPEANA_ID_1));

    String europeanaId = finder.findForFileUrl(METIS_DATASET_ID, FILE_URL);

    assertEquals(EUROPEANA_ID_1, europeanaId);
  }

  @Test
  public void shouldReturnIdSavedInHarvestedRecordsTableIfMoreThanOneUnambiguousEuropeanaIdsAreReturned()
      throws MalformedURLException, CloudException {
    idsFromUIS.add(createId(EUROPEANA_ID_1));
    idsFromUIS.add(createId(EUROPEANA_ID_3));
    when(harvestedRecordsDAO.findRecord(METIS_DATASET_ID, EUROPEANA_ID_3)).thenReturn(Optional.of(
        HarvestedRecord.builder().metisDatasetId(METIS_DATASET_ID).recordLocalId(EUROPEANA_ID_3).build()));

    String europeanaId = finder.findForFileUrl(METIS_DATASET_ID, FILE_URL);

    assertEquals(EUROPEANA_ID_3, europeanaId);
  }

  @Test(expected = TopologyGeneralException.class)
  public void shouldThrowExceptionIfMoreThanOneUnambiguousEuropeanaIdsAreReturnedAndAnyOfThemIsInHarvestedRecordsTable()
      throws MalformedURLException, CloudException {
    idsFromUIS.add(createId(EUROPEANA_ID_1));
    idsFromUIS.add(createId(EUROPEANA_ID_3));

    finder.findForFileUrl(METIS_DATASET_ID, FILE_URL);
  }

  @Test(expected = TopologyGeneralException.class)
  public void shouldThrowExceptionIfMoreThanOneUnambiguousEuropeanaIdsAreReturnedAndBothOfThemAreInHarvestedRecordsTable()
      throws MalformedURLException, CloudException {
    idsFromUIS.add(createId(EUROPEANA_ID_1));
    idsFromUIS.add(createId(EUROPEANA_ID_3));
    when(harvestedRecordsDAO.findRecord(METIS_DATASET_ID, EUROPEANA_ID_1)).thenReturn(Optional.of(
        HarvestedRecord.builder().metisDatasetId(METIS_DATASET_ID).recordLocalId(EUROPEANA_ID_1).build()));
    when(harvestedRecordsDAO.findRecord(METIS_DATASET_ID, EUROPEANA_ID_3)).thenReturn(Optional.of(
        HarvestedRecord.builder().metisDatasetId(METIS_DATASET_ID).recordLocalId(EUROPEANA_ID_3).build()));
    finder.findForFileUrl(METIS_DATASET_ID, FILE_URL);
  }

  private CloudId createId(String recordId) {
    CloudId cloudId = new CloudId();
    cloudId.setId(CLOUD_ID);
    LocalId localId = new LocalId();
    localId.setRecordId(recordId);
    localId.setProviderId(PROVIDER_ID);
    cloudId.setLocalId(localId);
    return cloudId;
  }


  private CloudException createRecordNotExistsException() {
    return new CloudException("RECORD_DOES_NOT_EXIST", new RecordDoesNotExistException(new ErrorInfo()));
  }

}