package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.common.model.*;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.utils.Bucket;
import eu.europeana.cloud.service.commons.utils.BucketsHandler;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;
import eu.europeana.cloud.service.mcs.persistent.context.SpiedServicesTestContext;
import org.hamcrest.core.Is;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.ByteArrayInputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO.DATA_SET_ASSIGNMENTS_BY_DATA_SET_BUCKETS;
import static eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO.DATA_SET_ASSIGNMENTS_BY_REVISION_ID_BUCKETS;
import static eu.europeana.cloud.service.mcs.persistent.cassandra.PersistenceUtils.createProviderDataSetId;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * @author sielski
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {SpiedServicesTestContext.class})
public class CassandraDataSetServiceTest extends CassandraTestBase {

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
  private static final String REVISION = "revision";
  private static final String VERSION= "7c91d370-5cf4-11ec-8000-d3ce7f0f06e3";
  private static final String REVISION_PROVIDER = "REVISION_PROVIDER";
  private static final String SAMPLE_PROVIDER_NAME = PROVIDER_ID;
  private static final String SAMPLE_DATASET_ID = DATA_SET_NAME;
  private static final String SAMPLE_REPRESENTATION_NAME_1 = "Sample_rep_1";
  private static final String SAMPLE_REPRESENTATION_NAME_2 = "Sample_rep_2";
  private static final String SAMPLE_REPRESENTATION_NAME_3 = "Sample_rep_3";
  private static final String SAMPLE_REVISION_NAME = "Revision_1";
  private static final String SAMPLE_REVISION_NAME2 = "Revision_2";
  private static final String SAMPLE_REVISION_PROVIDER = "Revision_Provider_1";
  private static final String SAMPLE_REVISION_PROVIDER2 = "Revision_Provider_2";
  private static final String SAMPLE_CLOUD_ID = "Cloud_1";
  private static final String SAMPLE_CLOUD_ID2 = "Cloud_2";
  private static final String SAMPLE_CLOUD_ID3 = "Cloud_3";
  private static final int MAX_DATASET_ASSIGNMENTS_BUCKET_COUNT = 100000;
  private static final int ASSIGNMENTS_COUNT = 1000;

  @After
  public void cleanUp() {
    Mockito.reset(uisHandler);
    Mockito.reset(dataSetDAO);
  }

  @Test
  public void shouldCreateDataSet() throws Exception {
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
  public void shouldCreateDataSetWithEmptyDescription() throws Exception {

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

  @Test(expected = DataSetNotExistsException.class)
  public void shouldNotAssignToNotExistingDataSet() throws Exception {
    makeUISProviderSuccess();
    // given all objects exist except for dataset
    Representation r = insertDummyPersistentRepresentation("cloud-id",
        "schema", PROVIDER_ID, "not-existing");

    // when trying to add assignment - error is expected
    cassandraDataSetService.addAssignment(PROVIDER_ID, "not-existing",
        r.getCloudId(), r.getRepresentationName(), r.getVersion());
  }

  @Test
  public void shouldAddAssignmentOnceForTheSameVersion() throws Exception {
    makeUISProviderSuccess();

    // given particular data set and representations
    String dsName = "ds";
    DataSet ds = cassandraDataSetService.createDataSet(PROVIDER_ID, dsName, "description of this set");
    Representation r1 = insertDummyPersistentRepresentation("cloud-id", "schema", PROVIDER_ID, ds.getId());

    // when representations are assigned to data set
    cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(), r1.getCloudId(),
        r1.getRepresentationName(), r1.getVersion());

    Bucket bucket = getCurrentDataSetAssignmentBucket(PROVIDER_ID, dsName);
    assertNotNull(bucket);
    assertEquals(1, bucket.getRowsCount());

    cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(), r1.getCloudId(),
        r1.getRepresentationName(), r1.getVersion());

    Bucket bucket2 = getCurrentDataSetAssignmentBucket(PROVIDER_ID, dsName);
    assertNotNull(bucket2);
    assertEquals(1, bucket2.getRowsCount());
    assertEquals(bucket.getBucketId(), bucket2.getBucketId());
  }

  @Test
  public void shouldRemoveAssignmentOnceForTheSameVersion() throws Exception {
    makeUISProviderSuccess();

    // given particular data set and representations
    String dsName = "ds";
    DataSet ds = cassandraDataSetService.createDataSet(PROVIDER_ID, dsName, "description of this set");
    Representation r1 = insertDummyPersistentRepresentation("cloud-id", "schema", PROVIDER_ID, ds.getId());
    Representation r2 = insertDummyPersistentRepresentation("cloud-id-2", "schema", PROVIDER_ID, ds.getId());

    // when representations are assigned to data set
    cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(), r1.getCloudId(),
        r1.getRepresentationName(), r1.getVersion());
    cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(), r2.getCloudId(),
        r2.getRepresentationName(), r2.getVersion());

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

  @Test(expected = RepresentationNotExistsException.class)
  public void shouldNotAssignNotExistingRepresentation() throws Exception {
    makeUISProviderSuccess();
    // given all objects exist except for representation
    DataSet ds = cassandraDataSetService.createDataSet(PROVIDER_ID, "ds",
        "description of this set");

    // when trying to add assignment - error is expected
    String version = new com.eaio.uuid.UUID().toString();
    cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(),
        "cloud-id", "schema", version);
  }

  @Test
  public void shouldAssignRepresentationsToDataSet()
      throws Exception {
    makeUISProviderSuccess();
    // given particular data set and representations
    String dsName = "ds";
    DataSet ds = cassandraDataSetService.createDataSet(PROVIDER_ID, dsName, "description of this set");
    Representation r1 = insertDummyPersistentRepresentation("cloud-id", "schema", PROVIDER_ID, ds.getId());

    Representation r2 = insertDummyPersistentRepresentation("cloud-id_1", "schema", PROVIDER_ID, ds.getId());

    // when representations are assigned to data set
    cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(), r1.getCloudId(),
        r1.getRepresentationName(), r1.getVersion());
    cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(), r2.getCloudId(),
        r2.getRepresentationName(), r2.getVersion());

    // then those representations should be returned when listing assignments
    List<Representation> assignedRepresentations = cassandraDataSetService.listDataSet(ds.getProviderId(),
        ds.getId(), null, 10000).getResults();

    assertThat(new HashSet<>(assignedRepresentations), is(new HashSet<>(Arrays.asList(r1, r2))));
  }


  @Test
  public void shouldRemoveAssignmentsFromDataSet() throws Exception {
    makeUISProviderSuccess();
    // given some representations in data set
    String dsName = "ds";
    DataSet ds = cassandraDataSetService.createDataSet(PROVIDER_ID, dsName,
        "description of this set");
    Representation r1 = insertDummyPersistentRepresentation("cloud-id",
        "schema", PROVIDER_ID, ds.getId());
    Representation r2 = insertDummyPersistentRepresentation("cloud-id_1",
        "schema", PROVIDER_ID, ds.getId());

    cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(),
        r1.getCloudId(), r1.getRepresentationName(), r1.getVersion());
    cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(),
        r2.getCloudId(), r2.getRepresentationName(), r2.getVersion());

    // when one of the representation is removed from data set
    cassandraDataSetService.removeAssignment(ds.getProviderId(),
        ds.getId(), r1.getCloudId(), r1.getRepresentationName(), r1.getVersion());

    // then only one representation should remain assigned in data set
    List<Representation> assignedRepresentations = cassandraDataSetService
        .listDataSet(ds.getProviderId(), ds.getId(), null, 10000)
        .getResults();
    assertThat(assignedRepresentations, is(Arrays.asList(r2)));
  }

  @Test(expected = DataSetDeletionException.class)
  public void shouldThrowExceptionForNonEmptyDataset() throws Exception {
    makeUISProviderSuccess();
    // given particular data set and representations in it
    String dsName = "ds";
    DataSet ds = cassandraDataSetService.createDataSet(PROVIDER_ID, dsName,
        "description of this set");
    Representation r1 = insertDummyPersistentRepresentation("cloud-id",
        "schema", PROVIDER_ID, ds.getId());
    Representation r2 = insertDummyPersistentRepresentation("cloud-id_1",
        "schema", PROVIDER_ID, ds.getId());
    cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(),
        r1.getCloudId(), r1.getRepresentationName(), r1.getVersion());
    cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(),
        r2.getCloudId(), r2.getRepresentationName(), r2.getVersion());

    // when this particular data set is removed
    cassandraDataSetService.deleteDataSet(ds.getProviderId(), ds.getId());
  }

  @Test(expected = DataSetNotExistsException.class)
  public void shouldThrowExceptionWhenDeletingNotExistingDataSet()
      throws Exception {
    cassandraDataSetService.deleteDataSet("xxx", "xxx");
  }

  @Test
  public void shouldAssignMostRecentVersionToDataSet() throws Exception {
    makeUISProviderSuccess();
    // given data set and multiple versions of the same representation
    String dsName = "ds";
    DataSet ds = cassandraDataSetService.createDataSet(PROVIDER_ID, dsName,
        "description of this set");
    Representation r1 = insertDummyPersistentRepresentation("cloud-id",
        "schema", PROVIDER_ID, ds.getId());
    insertDummyPersistentRepresentation("cloud-id", "schema", PROVIDER_ID, ds.getId());
    Representation r3 = insertDummyPersistentRepresentation("cloud-id",
        "schema", PROVIDER_ID, ds.getId());

    // when assigned representation without specyfying version
    cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(),
        r1.getCloudId(), r1.getRepresentationName(), null);

    // then the most recent version should be returned
    List<Representation> assignedRepresentations = cassandraDataSetService
        .listDataSet(ds.getProviderId(), ds.getId(), null, 10000)
        .getResults();
    assertThat(assignedRepresentations.size(), is(3));
  }

  @Test
  public void shouldCreateAndGetDataSet() throws Exception {
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
  public void shouldReturnPagedDataSets()
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

  @Test(expected = ProviderNotExistsException.class)
  public void shouldThrowExceptionWhenCreatingDatasetForNotExistingProvider()
      throws Exception {
    makeUISProviderFailure();
    cassandraDataSetService.createDataSet("not-existing-provider", "ds",
        "description");
  }

  @Test(expected = DataSetAlreadyExistsException.class)
  public void shouldNotCreateTwoDatasetsWithSameNameForProvider()
      throws Exception {
    String dsName = "ds";
    makeUISProviderSuccess();
    cassandraDataSetService
        .createDataSet(PROVIDER_ID, dsName, "description");
    cassandraDataSetService.createDataSet(PROVIDER_ID, dsName,
        "description of another");
  }


  private Representation insertDummyPersistentRepresentation(String cloudId,
      String schema, String providerId, String dataSetName) throws Exception {
    makeUISSuccess();
    makeUISProviderSuccess();
    Representation r = cassandraRecordService.createRepresentation(cloudId,
        schema, providerId, dataSetName);
    byte[] dummyContent = {1, 2, 3};
    File f = new File("content.xml", "application/xml", null, null, 0, null);
    cassandraRecordService.putContent(cloudId, schema, r.getVersion(), f,
        new ByteArrayInputStream(dummyContent));
    Representation persistRepresentation = cassandraRecordService
        .persistRepresentation(r.getCloudId(),
            r.getRepresentationName(), r.getVersion());
    return persistRepresentation;
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

  private void makeUISSuccess() throws RecordNotExistsException {
    Mockito.doReturn(true).when(uisHandler)
           .existsCloudId(Mockito.anyString());
  }

  private void makeDatasetExists() throws RecordNotExistsException {
    Mockito.doReturn(new DataSet()).when(dataSetDAO)
           .getDataSet(Mockito.anyString(), Mockito.anyString());
  }

  private void makeUISProviderExistsSuccess() {
    Mockito.doReturn(true).when(uisHandler).existsProvider(Mockito.anyString());
  }

  @Test
  public void shouldDeleteRevisionFromDataSet() throws Exception {
    makeUISProviderSuccess();
    makeUISSuccess();
    Date date = new Date();
    String cloudId = "2EEN23VWNXOW7LGLM6SKTDOZUBUOTKEWZ3IULSYEWEMERHISS6XA";

    DataSet ds = createDataset();
    Representation representation = insertDummyPersistentRepresentation(cloudId, REPRESENTATION, PROVIDER_ID, ds.getId());
    Revision revision = new Revision(REVISION, PROVIDER_ID, date, false);
    cassandraRecordService.addRevision(representation.getCloudId(),
        representation.getRepresentationName(), representation.getVersion(), revision);
    cassandraDataSetService.updateAllRevisionDatasetsEntries(representation.getCloudId(),
        representation.getRepresentationName(),
        representation.getVersion(), revision);

    cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(),
        representation.getCloudId(), representation.getRepresentationName(), representation.getVersion());

    ResultSlice<CloudTagsResponse> responseResultSlice = cassandraDataSetService.getDataSetsRevisions(ds.getProviderId(),
        ds.getId(), revision.getRevisionProviderId(), revision.getRevisionName(), revision.getCreationTimeStamp(),
        representation.getRepresentationName(), null, 100);
    assertNotNull(responseResultSlice.getResults());
    assertEquals(1, responseResultSlice.getResults().size());

    cassandraDataSetService.deleteRevision(cloudId, REPRESENTATION, representation.getVersion(), REVISION, PROVIDER_ID, date);

    responseResultSlice = cassandraDataSetService.getDataSetsRevisions(ds.getProviderId(), ds.getId(),
        revision.getRevisionProviderId(), revision.getRevisionName(), revision.getCreationTimeStamp(),
        representation.getRepresentationName(), null, 100);
    assertNotNull(responseResultSlice.getResults());
    assertEquals(0, responseResultSlice.getResults().size());

  }

  @Test
  public void shouldAllowReturningOnlyExistingRevisions() throws Exception {
    makeUISProviderSuccess();
    //given
    makeUISSuccess();
    makeDatasetExists();
    Revision exitstingRevision = new Revision(REVISION, REVISION_PROVIDER);
    Revision deletedRevision = new Revision(REVISION, REVISION_PROVIDER);
    deletedRevision.setCreationTimeStamp(exitstingRevision.getCreationTimeStamp());
    deletedRevision.setDeleted(true);
    for (int i = 0; i < 12500; i++) {
      if ((i + 1) % 2000 == 0) {
        cassandraDataSetService.addDataSetRevision(PROVIDER_ID, DATA_SET_NAME, exitstingRevision, REPRESENTATION,
            "CLOUD_ID_" + i, VERSION);
      } else {
        cassandraDataSetService.addDataSetRevision(PROVIDER_ID, DATA_SET_NAME, deletedRevision, REPRESENTATION, "CLOUD_ID_" + i, VERSION);
      }
    }

    //when
    List<CloudTagsResponse> result = cassandraDataSetService.getDataSetsExistingRevisions(PROVIDER_ID, DATA_SET_NAME,
        REVISION_PROVIDER, REVISION, exitstingRevision.getCreationTimeStamp(), REPRESENTATION, 5);
    //then
    assertEquals(5, result.size());
    result.forEach(response -> assertFalse(response.isDeleted()));

    //when
    result = cassandraDataSetService.getDataSetsExistingRevisions(PROVIDER_ID, DATA_SET_NAME, REVISION_PROVIDER,
        REVISION, exitstingRevision.getCreationTimeStamp(), REPRESENTATION, 6);
    //then
    assertEquals(6, result.size());
    result.forEach(response -> assertFalse(response.isDeleted()));

    //when
    result = cassandraDataSetService.getDataSetsExistingRevisions(PROVIDER_ID, DATA_SET_NAME, REVISION_PROVIDER,
        REVISION, exitstingRevision.getCreationTimeStamp(), REPRESENTATION, 10);
    //then
    assertEquals(6, result.size());
    result.forEach(response -> assertFalse(response.isDeleted()));
  }

  @Test(expected = RepresentationNotExistsException.class)
  public void shouldThrowRepresentationNotExistsException() throws Exception {
    makeUISProviderSuccess();
    makeUISSuccess();
    makeDatasetExists();

    Date date = new Date();
    String cloudId = "2EEN23VWNXOW7LGLM6SKTDOZUBUOTKEWZ3IULSYEWEMERHISS6XA";

    cassandraDataSetService.deleteRevision(cloudId, REPRESENTATION, "3d6381c0-a3cf-11e9-960f-fa163e8d4ae3", REVISION, PROVIDER_ID,
        date);

  }


  @Test
  public void shouldListAllCloudIdForGivenRevisionAndDataset()
      throws ProviderNotExistsException, DataSetNotExistsException, DataSetAlreadyExistsException {
    //given
    makeUISProviderSuccess();
    createDataset();
    Bucket bucket = createDatasetAssignmentRevisionIdBucket();
    Revision revision1 = new Revision(SAMPLE_REVISION_NAME, SAMPLE_REVISION_PROVIDER);
    Revision revision2 = new Revision(SAMPLE_REVISION_NAME2, SAMPLE_REVISION_PROVIDER2);
    dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket.getBucketId(), revision1,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID, VERSION);
    dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket.getBucketId(), revision1,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID2, VERSION);
    //assigned to different revision
    dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket.getBucketId(), revision2,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID3, VERSION);

    //when
    ResultSlice<CloudTagsResponse> result = cassandraDataSetService.getDataSetsRevisions(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID,
        SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME, revision1.getCreationTimeStamp(), SAMPLE_REPRESENTATION_NAME_1, null, 2);

    //then
    assertThat(result.getResults().size(), Is.is(2));
    List<String> ids = new ArrayList<>();
    ids.add(result.getResults().get(0).getCloudId());
    ids.add(result.getResults().get(1).getCloudId());

    assertThat(ids, hasItems(SAMPLE_CLOUD_ID, SAMPLE_CLOUD_ID2));
    assertThat(ids, not(hasItems(SAMPLE_CLOUD_ID3)));
  }

  @Test
  public void shouldListAllCloudIdForGivenRevisionAndDatasetFromDifferentBuckets()
      throws ProviderNotExistsException, DataSetNotExistsException, DataSetAlreadyExistsException {
    //given
    makeUISProviderSuccess();
    createDataset();
    Bucket bucket1 = createDatasetAssignmentRevisionIdBucket();
    Revision revision1 = new Revision(SAMPLE_REVISION_NAME, SAMPLE_REVISION_PROVIDER);
    Revision revision2 = new Revision(SAMPLE_REVISION_NAME2, SAMPLE_REVISION_PROVIDER2);
    dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket1.getBucketId(), revision1,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID, VERSION);
    Bucket bucket2 = createDatasetAssignmentRevisionIdBucket();
    dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket2.getBucketId(), revision1,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID2, VERSION);
    //assigned to different revision
    dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket1.getBucketId(), revision2,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID3, VERSION);

    //when
    ResultSlice<CloudTagsResponse> result = cassandraDataSetService.getDataSetsRevisions(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID,
        SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME, revision1.getCreationTimeStamp(), SAMPLE_REPRESENTATION_NAME_1, null, 2);

    //then
    assertThat(result.getResults().size(), Is.is(2));
    List<String> ids = new ArrayList<>();
    ids.add(result.getResults().get(0).getCloudId());
    ids.add(result.getResults().get(1).getCloudId());

    assertThat(ids, hasItems(SAMPLE_CLOUD_ID, SAMPLE_CLOUD_ID2));
    assertThat(ids, not(hasItems(SAMPLE_CLOUD_ID3)));
  }

  @Test
  public void shouldListAllCloudIdsForGivenRevisionAndDatasetFromDifferentBucketsWhenMoreDataThanOnePage()
      throws ProviderNotExistsException, DataSetNotExistsException, DataSetAlreadyExistsException {
    //given
    makeUISProviderSuccess();
    createDataset();
    Bucket bucket1 = createDatasetAssignmentRevisionIdBucket();
    Revision revision1 = new Revision(SAMPLE_REVISION_NAME, SAMPLE_REVISION_PROVIDER);
    dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket1.getBucketId(), revision1,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID, VERSION);
    Bucket bucket2 = createDatasetAssignmentRevisionIdBucket();
    dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket2.getBucketId(), revision1,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID2, VERSION);
    dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket2.getBucketId(), revision1,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID3, VERSION);

    //when
    ResultSlice<CloudTagsResponse> page1 = cassandraDataSetService.getDataSetsRevisions(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID,
        SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME, revision1.getCreationTimeStamp(), SAMPLE_REPRESENTATION_NAME_1, null, 2);
    ResultSlice<CloudTagsResponse> page2 = cassandraDataSetService.getDataSetsRevisions(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID,
        SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME, revision1.getCreationTimeStamp(), SAMPLE_REPRESENTATION_NAME_1,
        page1.getNextSlice(), 2);

    //then
    assertThat(page1.getResults().size(), Is.is(2));
    assertThat(page1.getResults().get(0).getCloudId(), Is.is(SAMPLE_CLOUD_ID));
    assertThat(page1.getResults().get(1).getCloudId(), Is.is(SAMPLE_CLOUD_ID2));
    Assert.assertNotNull(page1.getNextSlice());
    assertThat(page2.getResults().size(), Is.is(1));
    assertThat(page2.getResults().get(0).getCloudId(), Is.is(SAMPLE_CLOUD_ID3));
    Assert.assertNull(page2.getNextSlice());
  }

  @Test
  public void shouldListAllCloudIdForGivenRevisionAndDatasetFromDifferentBucketsWithLimit()
      throws ProviderNotExistsException, DataSetNotExistsException, DataSetAlreadyExistsException {
    //given
    makeUISProviderSuccess();
    createDataset();
    Bucket bucket1 = createDatasetAssignmentRevisionIdBucket();
    Revision revision1 = new Revision(SAMPLE_REVISION_NAME, SAMPLE_REVISION_PROVIDER);
    Revision revision2 = new Revision(SAMPLE_REVISION_NAME2, SAMPLE_REVISION_PROVIDER2);
    dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket1.getBucketId(), revision1,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID, VERSION);
    Bucket bucket2 = createDatasetAssignmentRevisionIdBucket();
    dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket2.getBucketId(), revision1,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID2, VERSION);
    //assigned to different revision
    dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket1.getBucketId(), revision2,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID3, VERSION);

    //when
    ResultSlice<CloudTagsResponse> page1 = cassandraDataSetService.getDataSetsRevisions(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID,
        SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME, revision1.getCreationTimeStamp(), SAMPLE_REPRESENTATION_NAME_1, null, 1);
    ResultSlice<CloudTagsResponse> page2 = cassandraDataSetService.getDataSetsRevisions(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID,
        SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME, revision1.getCreationTimeStamp(), SAMPLE_REPRESENTATION_NAME_1,
        page1.getNextSlice(), 1);

    //then
    assertThat(page1.getResults().size(), Is.is(1));
    assertThat(page1.getResults().get(0).getCloudId(), Is.is(SAMPLE_CLOUD_ID));
    assertThat(page2.getResults().size(), Is.is(1));
    assertThat(page2.getResults().get(0).getCloudId(), Is.is(SAMPLE_CLOUD_ID2));

  }

  @Test
  public void shouldListAllCloudIdForGivenRevisionAndDatasetWithLimit()
      throws ProviderNotExistsException, DataSetNotExistsException, DataSetAlreadyExistsException {
    //given
    makeUISProviderSuccess();
    createDataset();
    Bucket bucket = createDatasetAssignmentRevisionIdBucket();
    Revision revision1 = new Revision(SAMPLE_REVISION_NAME, SAMPLE_REVISION_PROVIDER);
    Revision revision2 = new Revision(SAMPLE_REVISION_NAME2, SAMPLE_REVISION_PROVIDER2);
    dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket.getBucketId(), revision1,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID, VERSION);
    dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket.getBucketId(), revision1,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID2, VERSION);
    //assigned to different revision
    dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket.getBucketId(), revision2,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID3, VERSION);

    //when
    ResultSlice<CloudTagsResponse> result = cassandraDataSetService.getDataSetsRevisions(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID,
        SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME, revision1.getCreationTimeStamp(), SAMPLE_REPRESENTATION_NAME_1, null, 1);

    //then
    assertThat(result.getResults().size(), is(1));
    assertEquals(SAMPLE_CLOUD_ID, result.getResults().get(0).getCloudId());
  }

  @Test
  public void shouldListAllCloudIdForGivenRevisionAndDatasetWithPagination()
      throws ProviderNotExistsException, DataSetNotExistsException, DataSetAlreadyExistsException {
    //given
    makeUISProviderSuccess();
    createDataset();
    Bucket bucket = createDatasetAssignmentRevisionIdBucket();
    Revision revision1 = new Revision(SAMPLE_REVISION_NAME, SAMPLE_REVISION_PROVIDER);
    dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket.getBucketId(), revision1,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID, VERSION);
    dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket.getBucketId(), revision1,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID2, VERSION);
    dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket.getBucketId(), revision1,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID3, VERSION);

    //when
    ResultSlice<CloudTagsResponse> result = cassandraDataSetService.getDataSetsRevisions(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID,
        SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME, revision1.getCreationTimeStamp(), SAMPLE_REPRESENTATION_NAME_1, null, 1);

    //then
    assertThat(result.getResults().size(), is(1));
    assertThat(result.getResults().get(0).getCloudId(), is(SAMPLE_CLOUD_ID));
    Assert.assertNotNull(result.getNextSlice());

    result = cassandraDataSetService.getDataSetsRevisions(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, SAMPLE_REVISION_PROVIDER,
        SAMPLE_REVISION_NAME, revision1.getCreationTimeStamp(), SAMPLE_REPRESENTATION_NAME_1, result.getNextSlice(), 1);
    assertThat(result.getResults().size(), is(1));
    assertThat(result.getResults().get(0).getCloudId(), is(SAMPLE_CLOUD_ID2));
    Assert.assertNotNull(result.getNextSlice());

    result = cassandraDataSetService.getDataSetsRevisions(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, SAMPLE_REVISION_PROVIDER,
        SAMPLE_REVISION_NAME, revision1.getCreationTimeStamp(), SAMPLE_REPRESENTATION_NAME_1, result.getNextSlice(), 1);
    assertThat(result.getResults().size(), is(1));
    assertThat(result.getResults().get(0).getCloudId(), is(SAMPLE_CLOUD_ID3));
    assertNull(result.getNextSlice());
  }

  @Test
  public void shouldListAllCloudIdForGivenRevisionForSecondRevision()
      throws ProviderNotExistsException, DataSetNotExistsException, DataSetAlreadyExistsException {
    //given
    makeUISProviderSuccess();
    createDataset();
    Bucket bucket = createDatasetAssignmentRevisionIdBucket();
    Revision revision1 = new Revision(SAMPLE_REVISION_NAME, SAMPLE_REVISION_PROVIDER);
    Revision revision2 = new Revision(SAMPLE_REVISION_NAME2, SAMPLE_REVISION_PROVIDER2);
    dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket.getBucketId(), revision1,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID, VERSION);
    dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket.getBucketId(), revision1,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID2, VERSION);
    //assigned to different revision
    dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket.getBucketId(), revision2,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID3, VERSION);

    //when
    ResultSlice<CloudTagsResponse> result = cassandraDataSetService.getDataSetsRevisions(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID,
        SAMPLE_REVISION_PROVIDER2, SAMPLE_REVISION_NAME2, revision2.getCreationTimeStamp(), SAMPLE_REPRESENTATION_NAME_1, null,
        3);

    //then
    assertThat(result.getResults().size(), is(1));
    assertEquals(SAMPLE_CLOUD_ID3, result.getResults().get(0).getCloudId());
  }

  @Test
  public void shouldListAllCloudIdForBigDataLimit()
      throws ProviderNotExistsException, DataSetNotExistsException, DataSetAlreadyExistsException {
    //given
    makeUISProviderSuccess();
    createDataset();
    Bucket bucket = createDatasetAssignmentRevisionIdBucket();
    Revision revision1 = new Revision(SAMPLE_REVISION_NAME, SAMPLE_REVISION_PROVIDER);
    List<String> savedCloudIds = IntStream.range(0, 15000).mapToObj(i -> "cloud_id_" + i).sorted().collect(Collectors.toList());

    for (String cloudId : savedCloudIds) {
      dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket.getBucketId(), revision1,
          SAMPLE_REPRESENTATION_NAME_1, cloudId, VERSION);
    }
    //when
    ResultSlice<CloudTagsResponse> result = cassandraDataSetService.getDataSetsRevisions(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID,
        SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME, revision1.getCreationTimeStamp(), SAMPLE_REPRESENTATION_NAME_1, null,
        10000);

    //then
    assertThat(result.getResults().size(), Is.is(10000));
    Set<String> resultCloudIds = result.getResults().stream().map(CloudTagsResponse::getCloudId).collect(Collectors.toSet());
    for (String savedId : savedCloudIds.subList(0, 10000)) {
      assertTrue(resultCloudIds.contains(savedId));
    }
  }

  @Test
  public void shouldRemoveRevisionFromDataSet()
      throws ProviderNotExistsException, DataSetNotExistsException, DataSetAlreadyExistsException {
    //given
    makeUISProviderSuccess();
    createDataset();
    Bucket bucket = createDatasetAssignmentRevisionIdBucket();
    Revision revision1 = new Revision(SAMPLE_REVISION_NAME, SAMPLE_REVISION_PROVIDER);
    dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket.getBucketId(), revision1,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID, VERSION);
    increaseDatasetAssignmentRevisionIdBucketSize(bucket);
    dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket.getBucketId(), revision1,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID2, VERSION);

    //when
    cassandraDataSetService.removeDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, revision1,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID, VERSION);

    //then
    ResultSlice<CloudTagsResponse> result = cassandraDataSetService.getDataSetsRevisions(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID,
        SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME, revision1.getCreationTimeStamp(), SAMPLE_REPRESENTATION_NAME_1, null, 3);
    assertThat(result.getResults().size(), is(1));
    assertEquals(SAMPLE_CLOUD_ID2, result.getResults().get(0).getCloudId());
  }

  @Test
  public void shouldRemoveRevisionFromDataSetSecondRevision()
      throws ProviderNotExistsException, DataSetNotExistsException, DataSetAlreadyExistsException {
    //given
    makeUISProviderSuccess();
    createDataset();
    Bucket bucket = createDatasetAssignmentRevisionIdBucket();
    Revision revision1 = new Revision(SAMPLE_REVISION_NAME, SAMPLE_REVISION_PROVIDER);
    dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket.getBucketId(), revision1,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID, VERSION);
    increaseDatasetAssignmentRevisionIdBucketSize(bucket);
    dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket.getBucketId(), revision1,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID2, VERSION);

    //when
    cassandraDataSetService.removeDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, revision1,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID2, VERSION);

    //then
    ResultSlice<CloudTagsResponse> result = cassandraDataSetService.getDataSetsRevisions(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID,
        SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME, revision1.getCreationTimeStamp(), SAMPLE_REPRESENTATION_NAME_1, null, 3);
    assertThat(result.getResults().size(), is(1));
    assertEquals(SAMPLE_CLOUD_ID, result.getResults().get(0).getCloudId());
  }

  @Test
  public void shouldRemoveAssigmentsOnRemoveWholeDataSet()
      throws ProviderNotExistsException, DataSetAlreadyExistsException, DataSetNotExistsException, DataSetDeletionException {
    //given
    makeUISProviderSuccess();
    createDataset();
    Bucket bucket = createDatasetAssignmentRevisionIdBucket();
    Revision revision1 = new Revision(SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME);
    dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket.getBucketId(), revision1,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID, VERSION);
    dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, bucket.getBucketId(), revision1,
        SAMPLE_REPRESENTATION_NAME_1, SAMPLE_CLOUD_ID2, VERSION);

    //when
    cassandraDataSetService.deleteDataSet(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID);

    //then
    ResultSlice<CloudTagsResponse> cloudIds = cassandraDataSetService.getDataSetsRevisionsPage(SAMPLE_PROVIDER_NAME,
        SAMPLE_DATASET_ID, SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME, revision1.getCreationTimeStamp(),
        SAMPLE_REPRESENTATION_NAME_1, null, 3);
    assertThat(cloudIds.getResults().size(), is(0));
  }


  @Test
  public void shouldRemoveCountFromAssignmentBucketsWhenRemovingAssignments()
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
  public void shouldListDataSetWithPagination()
      throws Exception {
    makeUISSuccess();
    makeUISProviderSuccess();
    makeDatasetExists();
    createDatasetAssignmentBucket();
    cassandraRecordService.createRepresentation(SAMPLE_CLOUD_ID, REPRESENTATION, SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID);
    cassandraRecordService.createRepresentation(SAMPLE_CLOUD_ID2, REPRESENTATION, SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID);
    createDatasetAssignmentBucket();
    cassandraRecordService.createRepresentation(SAMPLE_CLOUD_ID3, REPRESENTATION, SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID);

    ResultSlice<Representation> page1 = cassandraDataSetService.listDataSet(SAMPLE_PROVIDER_NAME,
        SAMPLE_DATASET_ID, null, 1);
    ResultSlice<Representation> page2 = cassandraDataSetService.listDataSet(SAMPLE_PROVIDER_NAME,
        SAMPLE_DATASET_ID, page1.getNextSlice(), 3);

    assertThat(page1.getResults(), hasSize(1));
    assertThat(page1.getResults().get(0).getCloudId(), is(SAMPLE_CLOUD_ID));
    assertNotNull(page1.getNextSlice());
    assertThat(page2.getResults(), hasSize(2));
    assertThat(page2.getResults().get(0).getCloudId(), is(SAMPLE_CLOUD_ID2));
    assertThat(page2.getResults().get(1).getCloudId(), is(SAMPLE_CLOUD_ID3));
    assertNull(page2.getNextSlice());
  }

  @Test
  public void shouldListDataSetWithPaginationLastPageLimitEqualAccessibleDataCount()
      throws Exception {
    makeUISSuccess();
    makeUISProviderSuccess();
    makeDatasetExists();
    createDatasetAssignmentBucket();
    cassandraRecordService.createRepresentation(SAMPLE_CLOUD_ID, REPRESENTATION, SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID);
    cassandraRecordService.createRepresentation(SAMPLE_CLOUD_ID2, REPRESENTATION, SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID);
    createDatasetAssignmentBucket();
    cassandraRecordService.createRepresentation(SAMPLE_CLOUD_ID3, REPRESENTATION, SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID);

    ResultSlice<Representation> page1 = cassandraDataSetService.listDataSet(SAMPLE_PROVIDER_NAME,
        SAMPLE_DATASET_ID, null, 1);
    ResultSlice<Representation> page2 = cassandraDataSetService.listDataSet(SAMPLE_PROVIDER_NAME,
        SAMPLE_DATASET_ID, page1.getNextSlice(), 2);

    assertThat(page1.getResults(), hasSize(1));
    assertThat(page1.getResults().get(0).getCloudId(), is(SAMPLE_CLOUD_ID));
    assertNotNull(page1.getNextSlice());
    assertThat(page2.getResults(), hasSize(2));
    assertThat(page2.getResults().get(0).getCloudId(), is(SAMPLE_CLOUD_ID2));
    assertThat(page2.getResults().get(1).getCloudId(), is(SAMPLE_CLOUD_ID3));
    assertNull(page2.getNextSlice());
  }

  @Test
  public void shouldListDataSetWithPaginationHittingExactlyBucketBorders()
      throws Exception {
    makeUISSuccess();
    makeUISProviderSuccess();
    makeDatasetExists();
    createDatasetAssignmentBucket();
    cassandraRecordService.createRepresentation(SAMPLE_CLOUD_ID, REPRESENTATION, SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID);
    createDatasetAssignmentBucket();
    cassandraRecordService.createRepresentation(SAMPLE_CLOUD_ID2, REPRESENTATION, SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID);

    ResultSlice<Representation> page1 = cassandraDataSetService.listDataSet(SAMPLE_PROVIDER_NAME,
        SAMPLE_DATASET_ID, null, 1);
    ResultSlice<Representation> page2 = cassandraDataSetService.listDataSet(SAMPLE_PROVIDER_NAME,
        SAMPLE_DATASET_ID, page1.getNextSlice(), 1);

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

  private Bucket createDatasetAssignmentRevisionIdBucket() {
    String providerDataSetId = createProviderDataSetId(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID);
    Bucket bucket = new Bucket(providerDataSetId, new com.eaio.uuid.UUID().toString(), 0);
    bucketsHandler.increaseBucketCount(DATA_SET_ASSIGNMENTS_BY_REVISION_ID_BUCKETS, bucket);
    return bucket;
  }

  private void createDatasetAssignmentBucket() {
    String providerDataSetId = createProviderDataSetId(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID);
    Bucket bucket = new Bucket(providerDataSetId, new com.eaio.uuid.UUID().toString(), 0);
    bucketsHandler.increaseBucketCount(DATA_SET_ASSIGNMENTS_BY_DATA_SET_BUCKETS, bucket);
  }

  private void increaseDatasetAssignmentRevisionIdBucketSize(Bucket bucket) {
    bucketsHandler.increaseBucketCount(DATA_SET_ASSIGNMENTS_BY_REVISION_ID_BUCKETS, bucket);
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
          "representation_" + i, dataSet.getProviderId(), dataSet.getId());
      cassandraDataSetService.addAssignment(dataSet.getProviderId(), dataSet.getId(), representation.getCloudId(),
          representation.getRepresentationName(), representation.getVersion());
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
