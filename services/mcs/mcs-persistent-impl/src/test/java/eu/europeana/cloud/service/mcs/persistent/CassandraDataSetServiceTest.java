package eu.europeana.cloud.service.mcs.persistent;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.persistent.exception.SystemException;

/**
 * 
 * @author sielski
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = { "classpath:/spiedServicesTestContext.xml" })
public class CassandraDataSetServiceTest extends CassandraTestBase {

  
    @Autowired
    private CassandraRecordService cassandraRecordService;

    @Autowired
    private CassandraDataSetService cassandraDataSetService;

    @Autowired
    private UISClientHandler uisHandler;
    
    private static final String providerId = "provider";


   


    @Test
    public void shouldCreateDataSet()
            throws Exception {
    	makeUISSuccess();
        // given properties of data set
        String dsName = "ds";
        String description = "description of data set";

        // when new data set is created
        DataSet ds = cassandraDataSetService.createDataSet(providerId, dsName, description);

        // the created data set should properties as given for construction
        assertThat(ds.getId(), is(dsName));
        assertThat(ds.getDescription(), is(description));
        assertThat(ds.getProviderId(), is(providerId));
    }


    @Test(expected = DataSetNotExistsException.class)
    public void shouldNotAssignToNotExistingDataSet()
            throws Exception {
    	makeUISSuccess();
        // given all objects exist except for dataset
        Representation r = insertDummyPersistentRepresentation("cloud-id", "schema", providerId);

        // when trying to add assignment - error is expected
        cassandraDataSetService.addAssignment(providerId, "not-existing", r.getRecordId(), r.getSchema(),
                r.getVersion());
    }


    @Test(expected = RepresentationNotExistsException.class)
    public void shouldNotAssignNotExistingRepresentation()
            throws Exception {
    	makeUISSuccess();
        // given all objects exist except for representation
        DataSet ds = cassandraDataSetService.createDataSet(providerId, "ds", "description of this set");

        // when trying to add assignment - error is expected
        String version = new com.eaio.uuid.UUID().toString();
        cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(), "cloud-id", "schema", version);
    }


    @Test
    public void shouldAssignRepresentationsToDataSet()
            throws Exception {
    	makeUISSuccess();
        // given particular data set and representations
        String dsName = "ds";
        DataSet ds = cassandraDataSetService.createDataSet(providerId, dsName, "description of this set");
        Representation r1 = insertDummyPersistentRepresentation("cloud-id", "schema", providerId);

        Representation r2 = insertDummyPersistentRepresentation("cloud-id_1", "schema", providerId);

        // when representations are assigned to data set
        cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(), r1.getRecordId(), r1.getSchema(),
            r1.getVersion());
        cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(), r2.getRecordId(), r2.getSchema(),
            r2.getVersion());

        // then those representations should be returned when listing assignments
        List<Representation> assignedRepresentations = cassandraDataSetService.listDataSet(ds.getProviderId(),
                ds.getId(), null, 10000).getResults();

        assertThat(new HashSet<>(assignedRepresentations), is(new HashSet<>(Arrays.asList(r1, r2))));
    }


    @Test
    public void shouldRemoveAssignmentsFromDataSet()
            throws Exception {
    	makeUISSuccess();
        // given some representations in data set
        String dsName = "ds";
        DataSet ds = cassandraDataSetService.createDataSet(providerId, dsName, "description of this set");
        Representation r1 = insertDummyPersistentRepresentation("cloud-id", "schema", providerId);
        Representation r2 = insertDummyPersistentRepresentation("cloud-id_1", "schema", providerId);

        cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(), r1.getRecordId(), r1.getSchema(),
            r1.getVersion());
        cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(), r2.getRecordId(), r2.getSchema(),
            r2.getVersion());

        // when one of the representation is removed from data set
        cassandraDataSetService.removeAssignment(ds.getProviderId(), ds.getId(), r1.getRecordId(), r1.getSchema());

        // then only one representation should remain assigned in data set
        List<Representation> assignedRepresentations = cassandraDataSetService.listDataSet(ds.getProviderId(),
                ds.getId(), null, 10000).getResults();
        assertThat(assignedRepresentations, is(Arrays.asList(r2)));
    }


    @Test
    public void shouldDeleteDataSetWithAssignments()
            throws Exception {
    	makeUISSuccess();
        // given particular data set and representations in it
        String dsName = "ds";
        DataSet ds = cassandraDataSetService.createDataSet(providerId, dsName, "description of this set");
        Representation r1 = insertDummyPersistentRepresentation("cloud-id", "schema", providerId);
        Representation r2 = insertDummyPersistentRepresentation("cloud-id_1", "schema", providerId);
        cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(), r1.getRecordId(), r1.getSchema(),
            r1.getVersion());
        cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(), r2.getRecordId(), r2.getSchema(),
            r2.getVersion());

        // when this particular data set is removed
        cassandraDataSetService.deleteDataSet(ds.getProviderId(), ds.getId());

        // then this data set no longer exists
        List<DataSet> dataSets = cassandraDataSetService.getDataSets(providerId, null, 10000).getResults();
        assertTrue(dataSets.isEmpty());

        // and, even after recreating data set with the same name, nothing is assigned to it
        ds = cassandraDataSetService.createDataSet(providerId, dsName, "description of this set");
        List<Representation> assignedRepresentations = cassandraDataSetService.listDataSet(ds.getProviderId(),
                ds.getId(), null, 10000).getResults();
        assertTrue(assignedRepresentations.isEmpty());

    }


    @Test
    public void shouldAssignMostRecentVersionToDataSet()
            throws Exception {
    	makeUISSuccess();
        // given data set and multiple versions of the same representation
        String dsName = "ds";
        DataSet ds = cassandraDataSetService.createDataSet(providerId, dsName, "description of this set");
        Representation r1 = insertDummyPersistentRepresentation("cloud-id", "schema", providerId);
        Representation r2 = insertDummyPersistentRepresentation("cloud-id", "schema", providerId);
        Representation r3 = insertDummyPersistentRepresentation("cloud-id", "schema", providerId);

        //when assigned representation without specyfying version
        cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(), r1.getRecordId(), r1.getSchema(), null);

        // then the most recent version should be returned
        List<Representation> assignedRepresentations = cassandraDataSetService.listDataSet(ds.getProviderId(),
                ds.getId(), null, 10000).getResults();
        assertThat(assignedRepresentations, is(Arrays.asList(r3)));
    }


    @Test
    public void shouldCreateAndGetDataSet()
            throws Exception {
    	makeUISSuccess();
        // given
        String dsName = "ds";
        DataSet ds = cassandraDataSetService.createDataSet(providerId, dsName, "description of this set");

        // when data sets for this provider are fetched
        List<DataSet> dataSets = cassandraDataSetService.getDataSets(providerId, null, 10000).getResults();

        // then those data sets should contain only the one inserted
        assertThat(dataSets.size(), is(1));
        assertThat(dataSets.get(0), is(ds));
    }


    @Test
    public void shouldReturnPagedDataSets()
            throws Exception {
    	makeUISSuccess();
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


    @Test(expected = ProviderDoesNotExistException.class)
    public void shouldThrowExceptionWhenListingDatasetsOfNotExistingProvider()
            throws Exception {
    	makeUISFailure();
        cassandraDataSetService.getDataSets("not-existing-provider", null, 10000);
    }


    @Test(expected = ProviderDoesNotExistException.class)
    public void shouldThrowExceptionWhenCreatingDatasetForNotExistingProvider()
            throws Exception {
    	makeUISFailure();
        cassandraDataSetService.createDataSet("not-existing-provider", "ds", "description");
    }


    @Test(expected = DataSetAlreadyExistsException.class)
    public void shouldNotCreateTwoDatasetsWithSameNameForProvider()
            throws Exception {
        String dsName = "ds";
        cassandraDataSetService.createDataSet(providerId, dsName, "description");
        cassandraDataSetService.createDataSet(providerId, dsName, "description of another");
    }


    private Representation insertDummyPersistentRepresentation(String cloudId, String schema, String providerId)
            throws Exception {
    	makeUISSuccess();
        Representation r = cassandraRecordService.createRepresentation(cloudId, schema, providerId);
        byte[] dummyContent = { 1, 2, 3 };
        File f = new File("content.xml", "application/xml", null, null, 0, null);
        cassandraRecordService.putContent(cloudId, schema, r.getVersion(), f, new ByteArrayInputStream(dummyContent));
        return cassandraRecordService.persistRepresentation(r.getRecordId(), r.getSchema(), r.getVersion());
    }

    private void makeUISSuccess()
            throws RecordNotExistsException {
        Mockito.reset(uisHandler);
        Mockito.doReturn(true).when(uisHandler).providerExistsInUIS(Mockito.anyString());
    }
    private void makeUISFailure()
            throws RecordNotExistsException {
        Mockito.reset(uisHandler);
        Mockito.doReturn(false).when(uisHandler).providerExistsInUIS(Mockito.anyString());
    }

  
}
