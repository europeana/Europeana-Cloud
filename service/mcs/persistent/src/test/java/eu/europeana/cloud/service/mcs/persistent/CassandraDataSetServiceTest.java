package eu.europeana.cloud.service.mcs.persistent;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import eu.europeana.cloud.common.model.*;
import eu.europeana.cloud.common.response.CloudVersionRevisionResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.utils.RevisionUtils;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.*;

import java.io.ByteArrayInputStream;
import java.util.*;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.*;

import eu.europeana.cloud.test.CassandraTestInstance;
import eu.europeana.cloud.service.uis.encoder.IdGenerator;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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

    private static final String providerId = "provider";


    private static final String DATA_SET_NAME = "dataset1";

    private static final String REPRESENTATION = "representation";

    private static final String REVISION = "revision";

    @After
    public void cleanUp() {
        Mockito.reset(uisHandler);
    }

    @Test
    public void shouldCreateDataSet() throws Exception {
        makeUISProviderSuccess();
        // given properties of data set
        String dsName = "ds";
        String description = "description of data set";

        // when new data set is created
        DataSet ds = cassandraDataSetService.createDataSet(providerId, dsName,
                description);

        // the created data set should properties as given for construction
        assertThat(ds.getId(), is(dsName));
        assertThat(ds.getDescription(), is(description));
        assertThat(ds.getProviderId(), is(providerId));
    }

    @Test
    public void shouldCreateDataSetWithEmptyDescription() throws Exception {

        makeUISProviderSuccess();
        // given properties of data set
        String dsName = "ds_empty_description";
        String description = null;

        // when new data set is created
        DataSet ds = cassandraDataSetService.createDataSet(providerId, dsName,
                description);

        ResultSlice<DataSet> dataSets = cassandraDataSetService.getDataSets(
                providerId, null, 50);
        List<DataSet> results = dataSets.getResults();
        assertTrue(results.contains(ds));
    }

    @Test(expected = DataSetNotExistsException.class)
    public void shouldNotAssignToNotExistingDataSet() throws Exception {
        makeUISProviderSuccess();
        // given all objects exist except for dataset
        Representation r = insertDummyPersistentRepresentation("cloud-id",
                "schema", providerId);

        // when trying to add assignment - error is expected
        cassandraDataSetService.addAssignment(providerId, "not-existing",
                r.getCloudId(), r.getRepresentationName(), r.getVersion());
    }

    @Test(expected = RepresentationNotExistsException.class)
    public void shouldNotAssignNotExistingRepresentation() throws Exception {
        makeUISProviderSuccess();
        // given all objects exist except for representation
        DataSet ds = cassandraDataSetService.createDataSet(providerId, "ds",
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
        DataSet ds = cassandraDataSetService.createDataSet(providerId, dsName, "description of this set");
        Representation r1 = insertDummyPersistentRepresentation("cloud-id", "schema", providerId);

        Representation r2 = insertDummyPersistentRepresentation("cloud-id_1", "schema", providerId);

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
        DataSet ds = cassandraDataSetService.createDataSet(providerId, dsName,
                "description of this set");
        Representation r1 = insertDummyPersistentRepresentation("cloud-id",
                "schema", providerId);
        Representation r2 = insertDummyPersistentRepresentation("cloud-id_1",
                "schema", providerId);

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
        DataSet ds = cassandraDataSetService.createDataSet(providerId, dsName,
                "description of this set");
        Representation r1 = insertDummyPersistentRepresentation("cloud-id",
                "schema", providerId);
        Representation r2 = insertDummyPersistentRepresentation("cloud-id_1",
                "schema", providerId);
        cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(),
                r1.getCloudId(), r1.getRepresentationName(), r1.getVersion());
        cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(),
                r2.getCloudId(), r2.getRepresentationName(), r2.getVersion());

        // when this particular data set is removed
        cassandraDataSetService.deleteDataSet(ds.getProviderId(), ds.getId());

        // then this data set no longer exists
        List<DataSet> dataSets = cassandraDataSetService.getDataSets(
                providerId, null, 10000).getResults();
        assertTrue(dataSets.isEmpty());

        // and, even after recreating data set with the same name, nothing is
        // assigned to it
        ds = cassandraDataSetService.createDataSet(providerId, dsName,
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
        DataSet ds = cassandraDataSetService.createDataSet(providerId, dsName,
                "description of this set");
        Representation r1 = insertDummyPersistentRepresentation("cloud-id",
                "schema", providerId);
        insertDummyPersistentRepresentation("cloud-id", "schema", providerId);
        Representation r3 = insertDummyPersistentRepresentation("cloud-id",
                "schema", providerId);

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
        DataSet ds = cassandraDataSetService.createDataSet(providerId, dsName,
                "description of this set");

        // when data sets for this provider are fetched
        List<DataSet> dataSets = cassandraDataSetService.getDataSets(
                providerId, null, 10000).getResults();

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
            DataSet ds = cassandraDataSetService.createDataSet(providerId, "ds_" + dsID, "description of " + dsID);
            insertedDataSetIds.add(ds.getId());
        }

        // iterate through all data set
        List<String> fetchedDataSets = new ArrayList<>(dataSetCount);
        int sliceSize = 10;
        String token = null;
        do {
            ResultSlice<DataSet> resultSlice = cassandraDataSetService.getDataSets(providerId, token, sliceSize);
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
                .createDataSet(providerId, dsName, "description");
        cassandraDataSetService.createDataSet(providerId, dsName,
                "description of another");
    }

    @Test
    public void shouldProperlyGetListOfDataSetsForGivenVersion() throws Exception {
        //given
        makeUISProviderSuccess();
        String dsName = "ds";
        DataSet ds = cassandraDataSetService.createDataSet(providerId, dsName,
                "description of this set");
        Representation r1 = insertDummyPersistentRepresentation("cloud-id",
                "schema", providerId);
        insertDummyPersistentRepresentation("cloud-id", "schema", providerId);
        cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(),
                r1.getCloudId(), r1.getRepresentationName(), r1.getVersion());

        //when
        Set<String> dataSets = cassandraDataSetService.getDataSets(ds.getProviderId(), r1.getCloudId(), r1.getRepresentationName
                (), r1.getVersion());
        //then
        assertThat(dataSets.size(), is(1));
        assertThat(dataSets, hasItem(dsName));
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
    }

    private void makeUISProviderFailure() {
        Mockito.doReturn(null).when(uisHandler)
                .getProvider(Mockito.anyString());
    }

    private void makeUISSuccess() throws RecordNotExistsException {
        Mockito.doReturn(true).when(uisHandler)
                .existsCloudId(Mockito.anyString());
    }

    private void makeUISFailure() throws RecordNotExistsException {
        Mockito.doReturn(false).when(uisHandler)
                .existsCloudId(Mockito.anyString());
    }

    private void makeUISProviderExistsSuccess() {
        Mockito.doReturn(true).when(uisHandler).existsProvider(Mockito.anyString());
    }

    private void makeUISProviderExistsFailure() {
        Mockito.doReturn(false).when(uisHandler).existsProvider(Mockito.anyString());
    }

    @Test(expected = DataSetNotExistsException.class)
    public void shouldThrowExceptionWhenRequestingCloudIdsForNonExistingDataSet()
            throws Exception {
        makeUISProviderExistsSuccess();
        cassandraDataSetService.getDataSetCloudIdsByRepresentationPublished("non-existent-ds", "provider", "representation", new Date(), null, 1);
    }

    @Test(expected = ProviderNotExistsException.class)
    public void shouldThrowExceptionWhenRequestingCloudIdsForNonExistingProvider()
            throws Exception {
        makeUISProviderExistsFailure();
        cassandraDataSetService.getDataSetCloudIdsByRepresentationPublished("ds", "non-existent-provider", "representation", new Date(), null, 1);
    }


    @Test
    public void shouldAddNewEntriesWhenUpdateInvoked()
            throws Exception {
        makeUISProviderSuccess();
        makeUISProviderExistsSuccess();

        // create dataset 1
        DataSet ds1 = cassandraDataSetService.createDataSet(providerId, "ds-1",
                "description of this set");
        // create dataset 2
        DataSet ds2 = cassandraDataSetService.createDataSet(providerId, "ds-2",
                "description of this set");
        // create dummy representation
        Representation r1 = insertDummyPersistentRepresentation("cloud-1",
                "schema", providerId);
        // assign version to dataset 1
        cassandraDataSetService.addAssignment(ds1.getProviderId(), ds1.getId(),
                r1.getCloudId(), r1.getRepresentationName(), r1.getVersion());
        // assign version to dataset 2
        cassandraDataSetService.addAssignment(ds2.getProviderId(), ds2.getId(),
                r1.getCloudId(), r1.getRepresentationName(), r1.getVersion());

        // create new revision (simulate normal API behaviour when creating a revision invokes update on the table)
        Revision r = new Revision("revision1", "rev_provider_1", new Date(), false, true, false);
        cassandraRecordService.addRevision(r1.getCloudId(), r1.getRepresentationName(), r1.getVersion(), r);
        cassandraDataSetService.updateProviderDatasetRepresentation(r1.getCloudId(), r1.getRepresentationName(), r1.getVersion(), r);

        // get date 1 day before
        Calendar c = Calendar.getInstance();
        c.add(Calendar.DAY_OF_MONTH, -1);

        // check whether update inserted row for dataset 1
        ResultSlice<CloudVersionRevisionResponse> cloudIds = cassandraDataSetService.getDataSetCloudIdsByRepresentationPublished(ds1.getId(), providerId, r1.getRepresentationName(), c.getTime(), null, 10);
        assertThat(cloudIds.getResults().size(), is(1));
        CloudVersionRevisionResponse resp = cloudIds.getResults().get(0);
        assertThat(resp.getCloudId(), is(r1.getCloudId()));
        assertThat(resp.getVersion(), is(r1.getVersion()));
        assertThat(resp.getRevisionId(), is(RevisionUtils.getRevisionKey(r)));

        // check whether update inserted row for dataset 2
        cloudIds = cassandraDataSetService.getDataSetCloudIdsByRepresentationPublished(ds2.getId(), providerId, r1.getRepresentationName(), c.getTime(), null, 10);
        assertThat(cloudIds.getResults().size(), is(1));
        resp = cloudIds.getResults().get(0);
        assertThat(resp.getCloudId(), is(r1.getCloudId()));
        assertThat(resp.getVersion(), is(r1.getVersion()));
        assertThat(resp.getRevisionId(), is(RevisionUtils.getRevisionKey(r)));
    }


    @Test
    public void shouldAddRemoveEntriesWhenAssignToDataset()
            throws Exception {
        makeUISProviderSuccess();
        makeUISProviderExistsSuccess();

        // create dataset 1
        DataSet ds1 = cassandraDataSetService.createDataSet(providerId, "ds-1",
                "description of this set");
        // create dummy representation
        Representation r1 = insertDummyPersistentRepresentation("cloud-1",
                "schema", providerId);

        // create revisions on the dummy version
        Revision r = new Revision("revision1", "rev_provider_1", new Date(), false, true, false);
        cassandraRecordService.addRevision(r1.getCloudId(), r1.getRepresentationName(), r1.getVersion(), r);
        r = new Revision("revision2", "rev_provider_2", new Date(), false, true, true);
        cassandraRecordService.addRevision(r1.getCloudId(), r1.getRepresentationName(), r1.getVersion(), r);

        // assign version to dataset 1
        cassandraDataSetService.addAssignment(ds1.getProviderId(), ds1.getId(),
                r1.getCloudId(), r1.getRepresentationName(), r1.getVersion());

        // get date 1 day before
        Calendar c = Calendar.getInstance();
        c.add(Calendar.DAY_OF_MONTH, -1);

        // check whether assignment created new entries in the table
        ResultSlice<CloudVersionRevisionResponse> cloudIds = cassandraDataSetService.getDataSetCloudIdsByRepresentationPublished(ds1.getId(), ds1.getProviderId(), r1.getRepresentationName(), c.getTime(), null, 10);
        // there should be one unique cloud id in the result list
        assertThat(new HashSet<>(cloudIds.getResults()).size(), is(2));
        CloudVersionRevisionResponse resp = cloudIds.getResults().get(0);
        assertThat(resp.getCloudId(), is(r1.getCloudId()));
        assertThat(resp.getVersion(), is(r1.getVersion()));
        assertThat(resp.getRevisionId(), is(RevisionUtils.getRevisionKey("rev_provider_1", "revision1")));

        resp = cloudIds.getResults().get(1);
        assertThat(resp.getCloudId(), is(r1.getCloudId()));
        assertThat(resp.getVersion(), is(r1.getVersion()));
        assertThat(resp.getRevisionId(), is(RevisionUtils.getRevisionKey("rev_provider_2", "revision2")));

        // remove assignment
        cassandraDataSetService.removeAssignment(ds1.getProviderId(), ds1.getId(), r1.getCloudId(), r1.getRepresentationName(), r1.getVersion());

        // check whether all assignments for the revisions associated with unassigned version were removed
        cloudIds = cassandraDataSetService.getDataSetCloudIdsByRepresentationPublished(ds1.getId(), ds1.getProviderId(), r1.getRepresentationName(), c.getTime(), null, 10);
        assertTrue(cloudIds.getResults().isEmpty());
    }


    @Test
    public void shouldReturnPagedCloudIds()
            throws Exception {
        // ensure that provider exists
        makeUISProviderSuccess();
        makeUISProviderExistsSuccess();
        // add data set
        cassandraDataSetService.createDataSet("provider1", "dataset1", "description");
        // create 1000 entries in the table
        int size = 1000;
        List<CloudVersionRevisionResponse> inserted = createDummyData(size);

        // get date 1 day before
        Calendar c = Calendar.getInstance();
        c.add(Calendar.DAY_OF_MONTH, -1);

        // get cloud ids in 50 element slices
        int sliceSize = 50;
        String token = null;
        List<CloudVersionRevisionResponse> retrieved = new ArrayList<>();
        do {
            ResultSlice<CloudVersionRevisionResponse> slice = cassandraDataSetService.getDataSetCloudIdsByRepresentationPublished("dataset1", "provider1", "representation", c.getTime(), token, sliceSize);
            token = slice.getNextSlice();
            // check whether token is null or the returned size is sliceSize
            assertTrue(slice.getResults().size() == sliceSize || token == null);
            retrieved.addAll(slice.getResults());
        } while (token != null);

        // check that the same cloud ids are on both list (inserted and retrieved)
        Collections.sort(inserted);
        Collections.sort(retrieved);
        assertThat(inserted, is(retrieved));
    }

    private List<CloudVersionRevisionResponse> createDummyData(int size) {
        Session session = CassandraTestInstance.getSession(KEYSPACE);

        // create prepared statement for entry in a table
        PreparedStatement ps = getPreparedStatementForInsertion(session);

        PreparedStatement psBuckets = getPreparedStatementForIBucketIdInsertion(session);

        // init size of table
        BoundStatement bs;
        BoundStatement bsBuckets;
        List<CloudVersionRevisionResponse> cloudIds = new ArrayList<>();

        String bucketId = new com.eaio.uuid.UUID().toString();
        // add new entries, each with different cloud id and revision timestamp
        for (int i = 0; i < size; i++) {
            CloudVersionRevisionResponse obj = new CloudVersionRevisionResponse(IdGenerator.encodeWithSha256AndBase32("/" + providerId + "/" + "cloud_" + i),
                    new com.eaio.uuid.UUID().toString(), RevisionUtils.getRevisionKey("revision", "revProvider"), true, false, false);
            bsBuckets = psBuckets.bind("provider1", "dataset1", UUID.fromString(bucketId));
            session.execute(bsBuckets);
            bs = ps.bind("provider1", "dataset1", UUID.fromString(bucketId), obj.getCloudId(), UUID.fromString(obj.getVersion()),
                    "representation", obj.getRevisionId(), new Date(), obj.isAcceptance(), obj.isPublished(), obj.isDeleted());
            session.execute(bs);
            cloudIds.add(obj);
        }
        return cloudIds;
    }

    private PreparedStatement getPreparedStatementForIBucketIdInsertion(Session session) {
        String bucketsTable = KEYSPACE + ".datasets_buckets";
        PreparedStatement psBuckets = session.prepare("UPDATE " + bucketsTable + " set rows_count = rows_count + 1 WHERE provider_id = ? AND dataset_id = ? AND bucket_id = ?;");
        psBuckets.setConsistencyLevel(ConsistencyLevel.QUORUM);
        return psBuckets;
    }


    @Test
    public void shouldDeleteDataSetCloudIdsByRepresentationWhenDeleteSet()
            throws Exception {
        makeUISProviderSuccess();
        makeUISProviderExistsSuccess();

        // create dataset 1
        DataSet ds1 = cassandraDataSetService.createDataSet(providerId, "ds-1",
                "description of this set");
        // create dummy representation
        Representation r1 = insertDummyPersistentRepresentation("cloud-1",
                "schema", providerId);

        // create revision on the dummy version
        Revision r = new Revision("revision1", "rev_provider_1", new Date(), false, true, false);
        cassandraRecordService.addRevision(r1.getCloudId(), r1.getRepresentationName(), r1.getVersion(), r);

        // assign version to dataset 1
        cassandraDataSetService.addAssignment(ds1.getProviderId(), ds1.getId(),
                r1.getCloudId(), r1.getRepresentationName(), r1.getVersion());

        // get date 1 day before
        Calendar c = Calendar.getInstance();
        c.add(Calendar.DAY_OF_MONTH, -1);

        // check whether assignment created new entries in the table
        ResultSlice<CloudVersionRevisionResponse> cloudIds = cassandraDataSetService.getDataSetCloudIdsByRepresentationPublished(ds1.getId(), ds1.getProviderId(), r1.getRepresentationName(), c.getTime(), null, 10);
        // there should be one element in the list
        assertThat(new HashSet<>(cloudIds.getResults()).size(), is(1));

        // data set is removed
        cassandraDataSetService.deleteDataSet(ds1.getProviderId(), ds1.getId());
        // retrieve all datasets
        List<DataSet> dataSets = cassandraDataSetService.getDataSets(
                providerId, null, 10000).getResults();
        // none should exist
        assertTrue(dataSets.isEmpty());

        // create data set again to avoid throwing DataSetNotExistsException
        ds1 = cassandraDataSetService.createDataSet(providerId, "ds-1",
                "description of this set");

        // retrieve info again
        cloudIds = cassandraDataSetService.getDataSetCloudIdsByRepresentationPublished(ds1.getId(), ds1.getProviderId(), r1.getRepresentationName(), c.getTime(), null, 10);
        assertTrue(cloudIds.getResults().isEmpty());
    }


    @Test
    public void shouldReturnTheLatestCloudIdsAndTimeStampInsideRevisionDataSet()
            throws Exception {
        int size = 10;
        prepareTestForTheLatestCloudIdAndTimeStampInsideDataSet(size);
        ResultSlice<CloudIdAndTimestampResponse> cloudIdsAndTimestampResponseResultSlice = cassandraDataSetService.getLatestDataSetCloudIdByRepresentationAndRevision(DATA_SET_NAME, providerId, REVISION, providerId, REPRESENTATION, null, 100);
        List<CloudIdAndTimestampResponse> cloudIdsAndTimestampResponse = cloudIdsAndTimestampResponseResultSlice.getResults();
        assertFalse(cloudIdsAndTimestampResponse.isEmpty());
        assertEquals(cloudIdsAndTimestampResponse.size(), size);
    }

    @Test
    public void shouldReturnTheLatestCloudIdsAndTimeStampInsideRevisionDataSetBatchByBatch()
            throws Exception {
        int size = 10;
        prepareTestForTheLatestCloudIdAndTimeStampInsideDataSet(size);

        ResultSlice<CloudIdAndTimestampResponse> cloudIdsAndTimestampResponseResultSlice = cassandraDataSetService.getLatestDataSetCloudIdByRepresentationAndRevision(DATA_SET_NAME, providerId, REVISION, providerId, REPRESENTATION, null, 1);
        List<CloudIdAndTimestampResponse> cloudIdsAndTimestampResponse = cloudIdsAndTimestampResponseResultSlice.getResults();
        assertFalse(cloudIdsAndTimestampResponse.isEmpty());
        assertEquals(cloudIdsAndTimestampResponse.size(), 1);
        String cloudId = cloudIdsAndTimestampResponse.get(0).getCloudId();

        cloudIdsAndTimestampResponseResultSlice = cassandraDataSetService.getLatestDataSetCloudIdByRepresentationAndRevision(DATA_SET_NAME, providerId, REVISION, providerId, REPRESENTATION, cloudId, 1);
        cloudIdsAndTimestampResponse = cloudIdsAndTimestampResponseResultSlice.getResults();
        int batchCounter = 1;
        while (!cloudIdsAndTimestampResponse.isEmpty()) {
            batchCounter++;
            cloudId = cloudIdsAndTimestampResponse.get(0).getCloudId();
            cloudIdsAndTimestampResponseResultSlice = cassandraDataSetService.getLatestDataSetCloudIdByRepresentationAndRevision(DATA_SET_NAME, providerId, REVISION, providerId, REPRESENTATION, cloudId, 1);
            cloudIdsAndTimestampResponse = cloudIdsAndTimestampResponseResultSlice.getResults();
        }
        assertEquals(batchCounter, size);
    }


    @Test(expected = DataSetNotExistsException.class)
    public void shouldThrowDataSetNotExistedException()
            throws Exception {
        int size = 10;
        prepareTestForTheLatestCloudIdAndTimeStampInsideDataSet(size);
        cassandraDataSetService.getLatestDataSetCloudIdByRepresentationAndRevision("Non_Existed_Dataset", providerId, REVISION, providerId, REPRESENTATION, null, 1);
    }


    private void prepareTestForTheLatestCloudIdAndTimeStampInsideDataSet(int rowNum) throws ProviderNotExistsException, DataSetAlreadyExistsException {
        makeUISProviderSuccess();
        makeUISProviderExistsSuccess();
        cassandraDataSetService.createDataSet(providerId, DATA_SET_NAME, "description");
        createDummyDataForLatestCloudIdsAndTimestampResponse(rowNum);
    }


    private void createDummyDataForLatestCloudIdsAndTimestampResponse(int size) {
        Session session = CassandraTestInstance.getSession(KEYSPACE);
        PreparedStatement ps = getPreparedStatementForInsertionToLatestCloudIdsAndTimestampTable(session);
        BoundStatement bs;
        for (int i = 0; i < size; i++) {
            Date date = new Date();
            CloudIdAndTimestampResponse obj = new CloudIdAndTimestampResponse(IdGenerator.encodeWithSha256AndBase32("/" + providerId + "/" + "cloud_" + i),
                    date);
            bs = ps.bind(providerId, DATA_SET_NAME, obj.getCloudId(), UUID.fromString(new com.eaio.uuid.UUID().toString()), REPRESENTATION, REVISION, providerId, date, true, false, false);
            session.execute(bs);
        }
    }


    private PreparedStatement getPreparedStatementForInsertion(Session session) {
        String table = KEYSPACE + ".provider_dataset_representation";
        PreparedStatement ps = session.prepare("INSERT INTO " + table + "(provider_id,dataset_id,bucket_id,cloud_id,version_id,representation_id,revision_id,revision_timestamp,acceptance,published,mark_deleted) VALUES " +
                "(?,?,?,?,?,?,?,?,?,?,?)");
        ps.setConsistencyLevel(ConsistencyLevel.QUORUM);
        return ps;
    }


    private PreparedStatement getPreparedStatementForInsertionToLatestCloudIdsAndTimestampTable(Session session) {
        String table = KEYSPACE + ".latest_provider_dataset_representation_revision";
        PreparedStatement ps = session.prepare("INSERT INTO " + table + "(provider_id,dataset_id,cloud_id,version_id,representation_id,revision_name,revision_provider,revision_timestamp,acceptance,published,mark_deleted) VALUES " +
                "(?,?,?,?,?,?,?,?,?,?,?)");
        ps.setConsistencyLevel(ConsistencyLevel.QUORUM);
        return ps;

    }
}
