package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.common.model.*;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.utils.Bucket;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.ByteArrayInputStream;
import java.util.*;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * @author sielski
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = {"classpath:/spiedServicesTestContext.xml"})
public class CassandraDataSetServiceTest extends CassandraTestBase {

    @Autowired
    private CassandraRecordService cassandraRecordService;

    @Autowired
    private CassandraDataSetService cassandraDataSetService;

    @Autowired
    private UISClientHandler uisHandler;

    @Autowired
    private CassandraDataSetDAO dataSetDAO;


    private static final String PROVIDER_ID = "provider";


    private static final String DATA_SET_NAME = "dataset1";

    private static final String REPRESENTATION = "representation";

    private static final String REVISION = "revision";

    private static final String REVISION_PROVIDER = "REVISION_PROVIDER";

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
                "schema", PROVIDER_ID);

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
        Representation r1 = insertDummyPersistentRepresentation("cloud-id", "schema", PROVIDER_ID);

        // when representations are assigned to data set
        cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(), r1.getCloudId(),
                r1.getRepresentationName(), r1.getVersion());

        Bucket bucket = dataSetDAO.getCurrentDataSetAssignmentBucket(PROVIDER_ID, dsName);
        assertNotNull(bucket);
        assertEquals(1, bucket.getRowsCount());

        cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(), r1.getCloudId(),
                r1.getRepresentationName(), r1.getVersion());

        Bucket bucket2 = dataSetDAO.getCurrentDataSetAssignmentBucket(PROVIDER_ID, dsName);
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
        Representation r1 = insertDummyPersistentRepresentation("cloud-id", "schema", PROVIDER_ID);
        Representation r2 = insertDummyPersistentRepresentation("cloud-id-2", "schema", PROVIDER_ID);

        // when representations are assigned to data set
        cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(), r1.getCloudId(),
                r1.getRepresentationName(), r1.getVersion());
        cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(), r2.getCloudId(),
                r2.getRepresentationName(), r2.getVersion());

        Bucket bucket = dataSetDAO.getCurrentDataSetAssignmentBucket(PROVIDER_ID, dsName);
        assertNotNull(bucket);
        assertEquals(2, bucket.getRowsCount());

        cassandraDataSetService.removeAssignment(ds.getProviderId(), ds.getId(), r1.getCloudId(),
                r1.getRepresentationName(), r1.getVersion());

        Bucket bucket2 = dataSetDAO.getCurrentDataSetAssignmentBucket(PROVIDER_ID, dsName);
        assertNotNull(bucket2);
        assertEquals(1, bucket2.getRowsCount());
        assertEquals(bucket.getBucketId(), bucket2.getBucketId());

        cassandraDataSetService.removeAssignment(ds.getProviderId(), ds.getId(), r1.getCloudId(),
                r1.getRepresentationName(), r1.getVersion());

        Bucket bucket3 = dataSetDAO.getCurrentDataSetAssignmentBucket(PROVIDER_ID, dsName);
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
        Representation r1 = insertDummyPersistentRepresentation("cloud-id", "schema", PROVIDER_ID);

        Representation r2 = insertDummyPersistentRepresentation("cloud-id_1", "schema", PROVIDER_ID);

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
                "schema", PROVIDER_ID);
        Representation r2 = insertDummyPersistentRepresentation("cloud-id_1",
                "schema", PROVIDER_ID);

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

    @Test
    public void shouldDeleteDataSetWithAssignments() throws Exception {
        makeUISProviderSuccess();
        // given particular data set and representations in it
        String dsName = "ds";
        DataSet ds = cassandraDataSetService.createDataSet(PROVIDER_ID, dsName,
                "description of this set");
        Representation r1 = insertDummyPersistentRepresentation("cloud-id",
                "schema", PROVIDER_ID);
        Representation r2 = insertDummyPersistentRepresentation("cloud-id_1",
                "schema", PROVIDER_ID);
        cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(),
                r1.getCloudId(), r1.getRepresentationName(), r1.getVersion());
        cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(),
                r2.getCloudId(), r2.getRepresentationName(), r2.getVersion());

        // when this particular data set is removed
        cassandraDataSetService.deleteDataSet(ds.getProviderId(), ds.getId());

        // then this data set no longer exists
        List<DataSet> dataSets = cassandraDataSetService.getDataSets(
                PROVIDER_ID, null, 10000).getResults();
        assertTrue(dataSets.isEmpty());

        // and, even after recreating data set with the same name, nothing is
        // assigned to it
        ds = cassandraDataSetService.createDataSet(PROVIDER_ID, dsName,
                "description of this set");
        List<Representation> assignedRepresentations = cassandraDataSetService
                .listDataSet(ds.getProviderId(), ds.getId(), null, 10000)
                .getResults();
        assertTrue(assignedRepresentations.isEmpty());

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
                "schema", PROVIDER_ID);
        insertDummyPersistentRepresentation("cloud-id", "schema", PROVIDER_ID);
        Representation r3 = insertDummyPersistentRepresentation("cloud-id",
                "schema", PROVIDER_ID);

        // when assigned representation without specyfying version
        cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(),
                r1.getCloudId(), r1.getRepresentationName(), null);

        // then the most recent version should be returned
        List<Representation> assignedRepresentations = cassandraDataSetService
                .listDataSet(ds.getProviderId(), ds.getId(), null, 10000)
                .getResults();
        assertThat(assignedRepresentations, is(Arrays.asList(r3)));
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
                                                               String schema, String providerId) throws Exception {
        makeUISSuccess();
        makeUISProviderSuccess();
        Representation r = cassandraRecordService.createRepresentation(cloudId,
                schema, providerId);
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
    public void shouldDeleteDataSetCloudIdsByRepresentationWhenDeleteSet() throws Exception {
        makeUISProviderSuccess();
        makeUISProviderExistsSuccess();

        // create dataset 1
        DataSet ds1 = cassandraDataSetService.createDataSet(PROVIDER_ID, "ds-1",
                "description of this set");
        // create dummy representation
        Representation r1 = insertDummyPersistentRepresentation("cloud-1",
                "schema", PROVIDER_ID);

        // create revision on the dummy version
        Revision r = new Revision("revision1", "rev_provider_1", new Date(), false, true, false);
        cassandraRecordService.addRevision(r1.getCloudId(), r1.getRepresentationName(), r1.getVersion(), r);

        // assign version to dataset 1
        cassandraDataSetService.addAssignment(ds1.getProviderId(), ds1.getId(),
                r1.getCloudId(), r1.getRepresentationName(), r1.getVersion());

        // get date 1 day before
        Calendar c = Calendar.getInstance();
        c.add(Calendar.DAY_OF_MONTH, -1);

        // data set is removed
        cassandraDataSetService.deleteDataSet(ds1.getProviderId(), ds1.getId());
        // retrieve all datasets
        List<DataSet> dataSets = cassandraDataSetService.getDataSets(
                PROVIDER_ID, null, 10000).getResults();
        // none should exist
        assertTrue(dataSets.isEmpty());

        // create data set again to avoid throwing DataSetNotExistsException
        ds1 = cassandraDataSetService.createDataSet(PROVIDER_ID, "ds-1",
                "description of this set");
    }

    @Test
    public void shouldDeleteRepresentationRevisionsFromDataSetsRevisionsTablesWhenDataSetIsDeleted()
            throws Exception {
        makeUISSuccess();
        makeUISProviderSuccess();
        String cloudId = "cloud-2";
        String representationName = "representation-1";

        // create new representation
        Representation r = cassandraRecordService.createRepresentation(cloudId,
                "representation-1", PROVIDER_ID);

        // create and add new revision
        Revision revision = new Revision(REVISION, REVISION_PROVIDER);
        revision.setPublished(true);
        cassandraRecordService.addRevision(r.getCloudId(),
                r.getRepresentationName(), r.getVersion(), revision);

        // add files to representation version
        byte[] dummyContent = {1, 2, 3};
        File f = new File("content.xml", "application/xml", null, null, 0, null);
        cassandraRecordService.putContent(cloudId, representationName, r.getVersion(), f,
                new ByteArrayInputStream(dummyContent));


        // given particular data set and representations in it
        String dsName = "ds";
        DataSet ds = cassandraDataSetService.createDataSet(PROVIDER_ID, dsName,
                "description of this set");

        cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(),
                r.getCloudId(), r.getRepresentationName(), r.getVersion());

        ResultSlice<Representation> representationResultSlice = cassandraDataSetService.listDataSet(PROVIDER_ID, dsName, null, 1000);
        List<Representation> representations = representationResultSlice.getResults();
        assertNotNull(representations);
        assertEquals(1, representations.size());

        ResultSlice<CloudTagsResponse> responseResultSlice = cassandraDataSetService.getDataSetsRevisions(ds.getProviderId(), ds.getId(), revision.getRevisionProviderId(), revision.getRevisionName(), revision.getCreationTimeStamp(), representationName, null, 100);
        assertNotNull(responseResultSlice.getResults());
        assertEquals(1, responseResultSlice.getResults().size());

        cassandraDataSetService.deleteDataSet(PROVIDER_ID, dsName);

        DataSet dataSet = dataSetDAO.getDataSet(PROVIDER_ID, dsName);
        assertNull(dataSet);
        makeDatasetExists();

        responseResultSlice = cassandraDataSetService.getDataSetsRevisions(ds.getProviderId(), ds.getId(), revision.getRevisionProviderId(), revision.getRevisionName(), revision.getCreationTimeStamp(), representationName, null, 100);
        assertNotNull(responseResultSlice.getResults());
        assertEquals(0, responseResultSlice.getResults().size());

        representationResultSlice = cassandraDataSetService.listDataSet(PROVIDER_ID, dsName, null, 1000);
        representations = representationResultSlice.getResults();
        assertNotNull(representations);
        assertEquals(0, representations.size());
    }


    @Test
    public void shouldDeleteRevisionFromDataSet() throws Exception {
        makeUISProviderSuccess();
        makeUISSuccess();
        Date date = new Date();
        String cloudId = "2EEN23VWNXOW7LGLM6SKTDOZUBUOTKEWZ3IULSYEWEMERHISS6XA";

        Representation representation = insertDummyPersistentRepresentation(cloudId, REPRESENTATION, PROVIDER_ID);
        DataSet ds = cassandraDataSetService.createDataSet(PROVIDER_ID, DATA_SET_NAME,
                "description of this set");
        Revision revision = new Revision(REVISION, PROVIDER_ID, date, true, true, false);
        cassandraRecordService.addRevision(representation.getCloudId(),
                representation.getRepresentationName(), representation.getVersion(), revision);
        cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(),
                representation.getCloudId(), representation.getRepresentationName(), representation.getVersion());

        ResultSlice<CloudTagsResponse> responseResultSlice = cassandraDataSetService.getDataSetsRevisions(ds.getProviderId(), ds.getId(), revision.getRevisionProviderId(), revision.getRevisionName(), revision.getCreationTimeStamp(), representation.getRepresentationName(), null, 100);
        assertNotNull(responseResultSlice.getResults());
        assertEquals(1, responseResultSlice.getResults().size());

        cassandraDataSetService.deleteRevision(cloudId, REPRESENTATION, representation.getVersion(), REVISION, PROVIDER_ID, date);

        responseResultSlice = cassandraDataSetService.getDataSetsRevisions(ds.getProviderId(), ds.getId(), revision.getRevisionProviderId(), revision.getRevisionName(), revision.getCreationTimeStamp(), representation.getRepresentationName(), null, 100);
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
        for(int i=0;i<12500;i++) {
            if ((i + 1) % 2000 == 0) {
                dataSetDAO.addDataSetsRevision(PROVIDER_ID, DATA_SET_NAME, exitstingRevision, REPRESENTATION, "CLOUD_ID_" + i);
            } else {
                dataSetDAO.addDataSetsRevision(PROVIDER_ID, DATA_SET_NAME, deletedRevision, REPRESENTATION, "CLOUD_ID_" + i);
            }
        }


        //when
        List<CloudTagsResponse> result = cassandraDataSetService.getDataSetsExistingRevisions(PROVIDER_ID, DATA_SET_NAME,
                REVISION_PROVIDER, REVISION, exitstingRevision.getCreationTimeStamp(), REPRESENTATION, 5);
        //then
        assertEquals(5,result.size());
        result.forEach(response->assertFalse(response.isDeleted()));


        //when
        result = cassandraDataSetService.getDataSetsExistingRevisions(PROVIDER_ID, DATA_SET_NAME, REVISION_PROVIDER,
                REVISION, exitstingRevision.getCreationTimeStamp(), REPRESENTATION, 6);
        //then
        assertEquals(6,result.size());
        result.forEach(response->assertFalse(response.isDeleted()));


        //when
        result = cassandraDataSetService.getDataSetsExistingRevisions(PROVIDER_ID, DATA_SET_NAME, REVISION_PROVIDER,
                REVISION, exitstingRevision.getCreationTimeStamp(), REPRESENTATION, 10);
        //then
        assertEquals(6,result.size());
        result.forEach(response->assertFalse(response.isDeleted()));
    }

    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsException() throws Exception {
        makeUISProviderSuccess();
        makeUISSuccess();
        makeDatasetExists();

        Date date = new Date();
        String cloudId = "2EEN23VWNXOW7LGLM6SKTDOZUBUOTKEWZ3IULSYEWEMERHISS6XA";

        cassandraDataSetService.deleteRevision(cloudId, REPRESENTATION, "3d6381c0-a3cf-11e9-960f-fa163e8d4ae3", REVISION, PROVIDER_ID, date);

    }


}
