package eu.europeana.cloud.service.mcs.persistent;

import com.datastax.driver.core.utils.UUIDs;
import eu.europeana.cloud.common.model.*;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.utils.Bucket;
import eu.europeana.cloud.service.commons.utils.BucketsHandler;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;
import eu.europeana.cloud.service.mcs.persistent.context.SpiedServicesTestContext;
import eu.europeana.cloud.test.S3TestHelper;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO.DATA_SET_ASSIGNMENTS_BY_DATA_SET_BUCKETS;
import static eu.europeana.cloud.service.mcs.persistent.cassandra.PersistenceUtils.createProviderDataSetId;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author sielski
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {SpiedServicesTestContext.class})
class CassandraDataSetServiceTest extends CassandraTestBase {

  public static final UUID SAMPLE_VERSION_1 = UUIDs.timeBased();
  public static final UUID SAMPLE_VERSION_2 = UUIDs.timeBased();
  public static final UUID SAMPLE_VERSION_3 = UUIDs.timeBased();
  @Autowired
  private CassandraRecordService cassandraRecordService;

  @Autowired
  private CassandraDataSetService cassandraDataSetService;

  @Autowired
  private UISClientHandler uisHandler;

  @Autowired
  private CassandraDataSetDAO dataSetDAO;

  @Autowired
  private BucketsHandler bucketsHandler;


  private static final String PROVIDER_ID = "provider";
  private static final String DATA_SET_NAME = "dataset1";
  private static final String REPRESENTATION = "representation";
  private static final String VERSION= "7c91d370-5cf4-11ec-8000-d3ce7f0f06e3";
  private static final String SAMPLE_PROVIDER_NAME = PROVIDER_ID;
  private static final String SAMPLE_DATASET_ID = DATA_SET_NAME;
  private static final String SAMPLE_CLOUD_ID = "Cloud_1";
  private static final String SAMPLE_CLOUD_ID2 = "Cloud_2";
  private static final String SAMPLE_CLOUD_ID3 = "Cloud_3";
  private static final int MAX_DATASET_ASSIGNMENTS_BUCKET_COUNT = 100000;
  private static final int ASSIGNMENTS_COUNT = 1000;

  @BeforeAll
  static void setUp() {
    S3TestHelper.startS3MockServer();
  }

  @BeforeEach
  void cleanUpBeforeTest() {
    S3TestHelper.cleanUpBetweenTests();
    Mockito.reset(uisHandler);
    Mockito.reset(dataSetDAO);
  }

  @AfterAll
  static void cleanUp() {
    S3TestHelper.stopS3MockServer();
  }

  @Test
  void shouldCreateDataSet() throws Exception {
    makeUISProviderSuccess();
    // given properties of data set
    String dsName = "ds";
    String description = "description of data set";

    // when new data set is created
    DataSet ds = cassandraDataSetService.createDataSet(PROVIDER_ID, dsName,
            description);

    // the created data set should properties as given for construction
    assertThat(ds.getId(), is(dsName));
    assertThat(ds.getDescription(), is(description));
    assertThat(ds.getProviderId(), is(PROVIDER_ID));
  }

  @Test
  void shouldCreateDataSetWithEmptyDescription() throws Exception {

    makeUISProviderSuccess();
    // given properties of data set
    String dsName = "ds_empty_description";
    String description = null;

    // when new data set is created
    DataSet ds = cassandraDataSetService.createDataSet(PROVIDER_ID, dsName,
            description);

    ResultSlice<DataSet> dataSets = cassandraDataSetService.getDataSets(
        PROVIDER_ID, null, 50);
    List<DataSet> results = dataSets.getResults();
    assertTrue(results.contains(ds));
  }

  @Test
  void shouldRemoveAssignmentOnceForTheSameVersion() throws Exception {
    makeUISProviderSuccess();

    // given particular data set and representations
    String dsName = "ds";
    DataSet ds = cassandraDataSetService.createDataSet(PROVIDER_ID, dsName, "description of this set");
    Representation r1 = insertDummyPersistentRepresentation("cloud-id", "schema", PROVIDER_ID, SAMPLE_VERSION_1, ds.getId());
    insertDummyPersistentRepresentation("cloud-id-2", "schema", PROVIDER_ID, SAMPLE_VERSION_2, ds.getId());


    Bucket bucket = getCurrentDataSetAssignmentBucket(PROVIDER_ID, dsName);
    assertNotNull(bucket);
    assertEquals(2, bucket.getRowsCount());

    cassandraDataSetService.removeAssignment(ds.getProviderId(), ds.getId(), r1.getCloudId(),
        r1.getRepresentationName(), r1.getVersion());

    Bucket bucket2 = getCurrentDataSetAssignmentBucket(PROVIDER_ID, dsName);
    assertNotNull(bucket2);
    assertEquals(1, bucket2.getRowsCount());
    assertEquals(bucket.getBucketId(), bucket2.getBucketId());

    cassandraDataSetService.removeAssignment(ds.getProviderId(), ds.getId(), r1.getCloudId(),
        r1.getRepresentationName(), r1.getVersion());

    Bucket bucket3 = getCurrentDataSetAssignmentBucket(PROVIDER_ID, dsName);
    assertNotNull(bucket3);
    assertEquals(1, bucket3.getRowsCount());
    assertEquals(bucket.getBucketId(), bucket3.getBucketId());
  }


  @Test
  void shouldAssignRepresentationsToDataSet()
          throws Exception {
    makeUISProviderSuccess();
    // given particular data set and representations
    String dsName = "ds";
    DataSet ds = cassandraDataSetService.createDataSet(PROVIDER_ID, dsName, "description of this set");
    Representation r1 = insertDummyPersistentRepresentation("cloud-id", "schema", PROVIDER_ID, SAMPLE_VERSION_1, ds.getId());

    Representation r2 = insertDummyPersistentRepresentation("cloud-id_1", "schema", PROVIDER_ID, SAMPLE_VERSION_2, ds.getId());

    // then those representations should be returned when listing assignments
    List<Representation> assignedRepresentations = cassandraDataSetService.listDataSet(ds.getProviderId(),
        ds.getId(), null, false, 10000).getResults();

    assertThat(new HashSet<>(assignedRepresentations), is(new HashSet<>(Arrays.asList(r1, r2))));
  }


  @Test
  void shouldRemoveAssignmentsFromDataSet() throws Exception {
    makeUISProviderSuccess();
    // given some representations in data set
    String dsName = "ds";
    DataSet ds = cassandraDataSetService.createDataSet(PROVIDER_ID, dsName,
            "description of this set");
    Representation r1 = insertDummyPersistentRepresentation("cloud-id",
            "schema", PROVIDER_ID, SAMPLE_VERSION_1, ds.getId());
    Representation r2 = insertDummyPersistentRepresentation("cloud-id_1",
            "schema", PROVIDER_ID, SAMPLE_VERSION_2, ds.getId());


    // when one of the representation is removed from data set
    cassandraDataSetService.removeAssignment(ds.getProviderId(),
            ds.getId(), r1.getCloudId(), r1.getRepresentationName(), r1.getVersion());

    // then only one representation should remain assigned in data set
    List<Representation> assignedRepresentations = cassandraDataSetService
            .listDataSet(ds.getProviderId(), ds.getId(), null, false, 10000)
            .getResults();
    assertThat(assignedRepresentations, is(Arrays.asList(r2)));
  }

  @Test
  void shouldThrowExceptionForNonEmptyDataset() throws Exception {
    makeUISProviderSuccess();
    // given particular data set and representations in it
    String dsName = "ds";
    DataSet ds = cassandraDataSetService.createDataSet(PROVIDER_ID, dsName,
            "description of this set");
    insertDummyPersistentRepresentation("cloud-id",
            "schema", PROVIDER_ID, SAMPLE_VERSION_1, ds.getId());
    insertDummyPersistentRepresentation("cloud-id_1",
            "schema", PROVIDER_ID, SAMPLE_VERSION_2, ds.getId());

    // when this particular data set is removed
    assertThrows(DataSetDeletionException.class,
            () -> cassandraDataSetService.deleteDataSet(ds.getProviderId(), ds.getId()));
  }

  @Test
  void shouldThrowExceptionWhenDeletingNotExistingDataSet() {
    assertThrows(DataSetNotExistsException.class,
            () -> cassandraDataSetService.deleteDataSet("xxx", "xxx"));
  }

  @Test
  void shouldAssignMostRecentVersionToDataSet() throws Exception {
    makeUISProviderSuccess();
    // given data set and multiple versions of the same representation
    String dsName = "ds";
    DataSet ds = cassandraDataSetService.createDataSet(PROVIDER_ID, dsName,
            "description of this set");
    insertDummyPersistentRepresentation("cloud-id",
            "schema", PROVIDER_ID, SAMPLE_VERSION_1, ds.getId());
    insertDummyPersistentRepresentation("cloud-id",
            "schema", PROVIDER_ID, SAMPLE_VERSION_2, ds.getId());
    insertDummyPersistentRepresentation("cloud-id",
            "schema", PROVIDER_ID, SAMPLE_VERSION_3, ds.getId());

    // then the most recent version should be returned
    List<Representation> assignedRepresentations = cassandraDataSetService
        .listDataSet(ds.getProviderId(), ds.getId(), null, false, 10000)
        .getResults();
    assertThat(assignedRepresentations.size(), is(3));
  }

  @Test
  void shouldCreateAndGetDataSet() throws Exception {
    makeUISProviderSuccess();
    // given
    String dsName = "ds";
    DataSet ds = cassandraDataSetService.createDataSet(PROVIDER_ID, dsName,
            "description of this set");

    // when data sets for this provider are fetched
    List<DataSet> dataSets = cassandraDataSetService.getDataSets(
            PROVIDER_ID, null, 10000).getResults();

    // then those data sets should contain only the one inserted
    assertThat(dataSets.size(), is(1));
    assertThat(dataSets.get(0), is(ds));
  }

  @Test
  void shouldReturnPagedDataSets()
          throws Exception {
    makeUISSuccess();
    makeUISProviderSuccess();
    int dataSetCount = 1000;
    List<String> insertedDataSetIds = new ArrayList<>(dataSetCount);

    // insert random data sets
    for (int dsID = 0; dsID < dataSetCount; dsID++) {
      DataSet ds = cassandraDataSetService.createDataSet(PROVIDER_ID, "ds_" + dsID, "description of " + dsID);
      insertedDataSetIds.add(ds.getId());
    }

    // iterate through all data set
    List<String> fetchedDataSets = new ArrayList<>(dataSetCount);
    int sliceSize = 10;
    String token = null;
    do {
      ResultSlice<DataSet> resultSlice = cassandraDataSetService.getDataSets(PROVIDER_ID, token, sliceSize);
      token = resultSlice.getNextSlice();
      assertTrue(resultSlice.getResults().size() == sliceSize || token == null);
      for (DataSet ds : resultSlice.getResults()) {
        fetchedDataSets.add(ds.getId());
      }
    } while (token != null);

    Collections.sort(insertedDataSetIds);
    Collections.sort(fetchedDataSets);
    assertThat(insertedDataSetIds, is(fetchedDataSets));
  }

  @Test
  void shouldThrowExceptionWhenCreatingDatasetForNotExistingProvider() {
    makeUISProviderFailure();
    assertThrows(ProviderNotExistsException.class,
            () -> cassandraDataSetService.createDataSet("not-existing-provider", "ds",
                    "description"));
  }

  @Test
  void shouldNotCreateTwoDatasetsWithSameNameForProvider()
          throws Exception {
    String dsName = "ds";
    makeUISProviderSuccess();
    cassandraDataSetService
            .createDataSet(PROVIDER_ID, dsName, "description");
    assertThrows(DataSetAlreadyExistsException.class,
            () -> cassandraDataSetService.createDataSet(PROVIDER_ID, dsName, "description of another"));
  }


  private Representation insertDummyPersistentRepresentation(String cloudId,
      String schema, String providerId, UUID version, String dataSetName) throws Exception {
    makeUISSuccess();
    makeUISProviderSuccess();
    Representation r = cassandraRecordService.createRepresentation(cloudId,
        schema, providerId, version, dataSetName);
    byte[] dummyContent = {1, 2, 3};
    File f = new File("content.xml", "application/xml", null, null, 0, null);
    cassandraRecordService.putContent(cloudId, schema, r.getVersion(), f,
        new ByteArrayInputStream(dummyContent));
    return cassandraRecordService
        .persistRepresentation(r.getCloudId(),
            r.getRepresentationName(), r.getVersion());
  }

  private void makeUISProviderSuccess() {
    Mockito.doReturn(new DataProvider()).when(uisHandler)
           .getProvider(Mockito.anyString());
    Mockito.when(uisHandler.existsProvider(Mockito.anyString())).thenReturn(true);

  }

  private void makeUISProviderFailure() {
    Mockito.doReturn(null).when(uisHandler)
           .getProvider(Mockito.anyString());
  }

  private void makeUISSuccess() {
    Mockito.doReturn(true).when(uisHandler)
           .existsCloudId(Mockito.anyString());
  }

  private void makeDatasetExists() {
    Mockito.doReturn(new DataSet()).when(dataSetDAO)
           .getDataSet(Mockito.anyString(), Mockito.anyString());
  }

  @Test
  void shouldRemoveCountFromAssignmentBucketsWhenRemovingAssignments()
          throws DataSetNotExistsException, RepresentationNotExistsException, ProviderNotExistsException, DataSetAlreadyExistsException, RecordNotExistsException, DataSetAssignmentException {
    // given
    makeUISSuccess();
    makeUISProviderSuccess();
    DataSet dataSet = createDataset();

    // when
    List<Representation> assigned = prepareAssignedRepresentations(dataSet);
    Bucket bucket = getCurrentDataSetAssignmentBucket(dataSet.getProviderId(), dataSet.getId());

    // then
    assertNotNull(bucket);
    assertThat(bucket.getRowsCount(), is((long) (ASSIGNMENTS_COUNT % MAX_DATASET_ASSIGNMENTS_BUCKET_COUNT)));

    // when
    removeAssignments(dataSet, assigned);
    bucket = getCurrentDataSetAssignmentBucket(dataSet.getProviderId(), dataSet.getId());

    // then
    assertNull(bucket);
  }


  @Test
  void shouldListDataSetWithPagination()
          throws Exception {
    makeUISSuccess();
    makeUISProviderSuccess();
    makeDatasetExists();
    createDatasetAssignmentBucket();
    cassandraRecordService.createRepresentation(SAMPLE_CLOUD_ID, REPRESENTATION, SAMPLE_PROVIDER_NAME, SAMPLE_VERSION_1, SAMPLE_DATASET_ID);
    cassandraRecordService.createRepresentation(SAMPLE_CLOUD_ID2, REPRESENTATION, SAMPLE_PROVIDER_NAME, SAMPLE_VERSION_2, SAMPLE_DATASET_ID);
    createDatasetAssignmentBucket();
    cassandraRecordService.createRepresentation(SAMPLE_CLOUD_ID3, REPRESENTATION, SAMPLE_PROVIDER_NAME, SAMPLE_VERSION_3, SAMPLE_DATASET_ID);

    ResultSlice<Representation> page1 = cassandraDataSetService.listDataSet(SAMPLE_PROVIDER_NAME,
        SAMPLE_DATASET_ID, null, false, 1);
    ResultSlice<Representation> page2 = cassandraDataSetService.listDataSet(SAMPLE_PROVIDER_NAME,
        SAMPLE_DATASET_ID, page1.getNextSlice(), false, 3);

    assertThat(page1.getResults(), hasSize(1));
    assertThat(page1.getResults().get(0).getCloudId(), is(SAMPLE_CLOUD_ID));
    assertNotNull(page1.getNextSlice());
    assertThat(page2.getResults(), hasSize(2));
    assertThat(page2.getResults().get(0).getCloudId(), is(SAMPLE_CLOUD_ID2));
    assertThat(page2.getResults().get(1).getCloudId(), is(SAMPLE_CLOUD_ID3));
    assertNull(page2.getNextSlice());
  }


  @Test
  public void shouldListDataSetWithFilterOnExistingOnlyRepresentations()
          throws Exception {
    makeUISSuccess();
    makeUISProviderSuccess();
    makeDatasetExists();
    createDatasetAssignmentBucket();
    cassandraRecordService.createRepresentation(SAMPLE_CLOUD_ID, REPRESENTATION, SAMPLE_PROVIDER_NAME, SAMPLE_VERSION_1,SAMPLE_DATASET_ID);
    cassandraRecordService.createRepresentation(SAMPLE_CLOUD_ID2, REPRESENTATION, SAMPLE_PROVIDER_NAME, SAMPLE_VERSION_2, SAMPLE_DATASET_ID);
    createDatasetAssignmentBucket();
    cassandraRecordService.createRepresentation(SAMPLE_CLOUD_ID3, REPRESENTATION, SAMPLE_PROVIDER_NAME, SAMPLE_VERSION_3, SAMPLE_DATASET_ID);
    File sampleFile = prepareSampleFile();
    cassandraRecordService.putContent(SAMPLE_CLOUD_ID, REPRESENTATION, SAMPLE_VERSION_1.toString(), sampleFile, new ByteArrayInputStream("Example_content".getBytes(StandardCharsets.UTF_8)));

    ResultSlice<Representation> page1 = cassandraDataSetService.listDataSet(SAMPLE_PROVIDER_NAME,
            SAMPLE_DATASET_ID, null, true, 5000);

    assertThat(page1.getResults(), hasSize(1));
    assertThat(page1.getResults().get(0).getCloudId(), is(SAMPLE_CLOUD_ID));

    ResultSlice<Representation> page2 = cassandraDataSetService.listDataSet(SAMPLE_PROVIDER_NAME,
            SAMPLE_DATASET_ID, null, false, 5000);

    assertThat(page2.getResults(), hasSize(3));
    assertThat(page2.getResults().get(0).getCloudId(), is(SAMPLE_CLOUD_ID));
    assertThat(page2.getResults().get(1).getCloudId(), is(SAMPLE_CLOUD_ID2));
    assertThat(page2.getResults().get(2).getCloudId(), is(SAMPLE_CLOUD_ID3));
  }

  @NotNull
  private static File prepareSampleFile() {
    File sampleFile = new File();
    sampleFile.setFileName("SAMPLE_FILE");
    return sampleFile;
  }

  @Test
  void shouldListDataSetWithPaginationLastPageLimitEqualAccessibleDataCount()
          throws Exception {
    makeUISSuccess();
    makeUISProviderSuccess();
    makeDatasetExists();
    createDatasetAssignmentBucket();
    cassandraRecordService.createRepresentation(SAMPLE_CLOUD_ID, REPRESENTATION, SAMPLE_PROVIDER_NAME, SAMPLE_VERSION_1, SAMPLE_DATASET_ID);
    cassandraRecordService.createRepresentation(SAMPLE_CLOUD_ID2, REPRESENTATION, SAMPLE_PROVIDER_NAME, SAMPLE_VERSION_2, SAMPLE_DATASET_ID);
    createDatasetAssignmentBucket();
    cassandraRecordService.createRepresentation(SAMPLE_CLOUD_ID3, REPRESENTATION, SAMPLE_PROVIDER_NAME, SAMPLE_VERSION_3, SAMPLE_DATASET_ID);

    ResultSlice<Representation> page1 = cassandraDataSetService.listDataSet(SAMPLE_PROVIDER_NAME,
        SAMPLE_DATASET_ID, null, false, 1);
    ResultSlice<Representation> page2 = cassandraDataSetService.listDataSet(SAMPLE_PROVIDER_NAME,
        SAMPLE_DATASET_ID, page1.getNextSlice(), false, 2);

    assertThat(page1.getResults(), hasSize(1));
    assertThat(page1.getResults().get(0).getCloudId(), is(SAMPLE_CLOUD_ID));
    assertNotNull(page1.getNextSlice());
    assertThat(page2.getResults(), hasSize(2));
    assertThat(page2.getResults().get(0).getCloudId(), is(SAMPLE_CLOUD_ID2));
    assertThat(page2.getResults().get(1).getCloudId(), is(SAMPLE_CLOUD_ID3));
    assertNull(page2.getNextSlice());
  }

  @Test
  void shouldListDataSetWithPaginationHittingExactlyBucketBorders()
          throws Exception {
    makeUISSuccess();
    makeUISProviderSuccess();
    makeDatasetExists();
    createDatasetAssignmentBucket();
    cassandraRecordService.createRepresentation(SAMPLE_CLOUD_ID, REPRESENTATION, SAMPLE_PROVIDER_NAME, SAMPLE_VERSION_1, SAMPLE_DATASET_ID);
    createDatasetAssignmentBucket();
    cassandraRecordService.createRepresentation(SAMPLE_CLOUD_ID2, REPRESENTATION, SAMPLE_PROVIDER_NAME, SAMPLE_VERSION_2, SAMPLE_DATASET_ID);

    ResultSlice<Representation> page1 = cassandraDataSetService.listDataSet(SAMPLE_PROVIDER_NAME,
        SAMPLE_DATASET_ID, null, false, 1);
    ResultSlice<Representation> page2 = cassandraDataSetService.listDataSet(SAMPLE_PROVIDER_NAME,
        SAMPLE_DATASET_ID, page1.getNextSlice(), false, 1);

    assertThat(page1.getResults(), hasSize(1));
    assertThat(page1.getResults().get(0).getCloudId(), is(SAMPLE_CLOUD_ID));
    assertNotNull(page1.getNextSlice());
    assertThat(page2.getResults(), hasSize(1));
    assertThat(page2.getResults().get(0).getCloudId(), is(SAMPLE_CLOUD_ID2));
    assertNull(page2.getNextSlice());
  }

  private DataSet createDataset() throws ProviderNotExistsException, DataSetAlreadyExistsException {
    return cassandraDataSetService.createDataSet(PROVIDER_ID, DATA_SET_NAME, "description of this set");
  }

  private void createDatasetAssignmentBucket() {
    String providerDataSetId = createProviderDataSetId(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID);
    Bucket bucket = new Bucket(providerDataSetId, new com.eaio.uuid.UUID().toString(), 0);
    bucketsHandler.increaseBucketCount(DATA_SET_ASSIGNMENTS_BY_DATA_SET_BUCKETS, bucket);
  }

  private void removeAssignments(DataSet dataSet, List<Representation> assigned) throws DataSetNotExistsException {
    for (Representation representation : assigned) {
      cassandraDataSetService.removeAssignment(dataSet.getProviderId(), dataSet.getId(), representation.getCloudId(),
          representation.getRepresentationName(), representation.getVersion());
    }
  }

  private List<Representation> prepareAssignedRepresentations(DataSet dataSet)
      throws DataSetNotExistsException, RepresentationNotExistsException, ProviderNotExistsException, RecordNotExistsException, DataSetAssignmentException {
    List<Representation> assigned = new ArrayList<>();
    for (int i = 0; i < ASSIGNMENTS_COUNT; i++) {
      Representation representation = cassandraRecordService.createRepresentation("cloud_id_" + i,
          "representation_" + i, dataSet.getProviderId(), SAMPLE_VERSION_1, dataSet.getId());
      assigned.add(representation);
    }
    return assigned;
  }
//
  private Bucket getCurrentDataSetAssignmentBucket(String providerId, String datasetId) {
    return bucketsHandler.getCurrentBucket(
        DATA_SET_ASSIGNMENTS_BY_DATA_SET_BUCKETS, createProviderDataSetId(providerId, datasetId));
  }

}
