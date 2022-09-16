package eu.europeana.cloud.service.mcs.persistent;

import com.datastax.driver.core.utils.UUIDs;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.utils.Bucket;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.*;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = {"classpath:/spiedServicesTestContext.xml"})
public class CassandraDataSetDAOTest extends CassandraTestBase {

    @Autowired
    private CassandraDataSetDAO dataSetDAO;

    private static final String SAMPLE_PROVIDER_NAME = "Provider_1";
    private static final String SAMPLE_DATASET_ID = "Sample_ds_id_1";
    private static final String SAMPLE_REP_NAME_1 = "Sample_rep_1";
    private static final String SAMPLE_REP_NAME_2 = "Sample_rep_2";
    private static final String SAMPLE_REP_NAME_3 = "Sample_rep_3";


    private static final int MAX_DATASET_ASSIGNMENTS_BUCKET_COUNT = 100000;

    private static final int ASSIGNMENTS_COUNT = 1000;

    @Test
    public void newRepresentationNameShouldBeAdded() {
        dataSetDAO.addDataSetsRepresentationName(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, SAMPLE_REP_NAME_1);
        Set<String> repNames = dataSetDAO.getAllRepresentationsNamesForDataSet(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID);
        Assert.assertTrue(repNames.size() == 1);
        Assert.assertTrue(repNames.contains(SAMPLE_REP_NAME_1));
    }


    @Test
    public void representationNameShouldBeRemovedFromDB() {
        dataSetDAO.addDataSetsRepresentationName(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, SAMPLE_REP_NAME_1);
        dataSetDAO.addDataSetsRepresentationName(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, SAMPLE_REP_NAME_2);
        //
        dataSetDAO.removeRepresentationNameForDataSet(SAMPLE_REP_NAME_1, SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID);
        Set<String> repNames = dataSetDAO.getAllRepresentationsNamesForDataSet(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID);
        //
        Assert.assertTrue(repNames.size() == 1);
        Assert.assertTrue(repNames.contains(SAMPLE_REP_NAME_2));
        Assert.assertFalse(repNames.contains(SAMPLE_REP_NAME_1));
    }

    @Test
    public void allRepresentationsNamesForDataSetShouldBeRemoved() {
        dataSetDAO.addDataSetsRepresentationName(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, SAMPLE_REP_NAME_1);
        dataSetDAO.addDataSetsRepresentationName(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, SAMPLE_REP_NAME_2);
        dataSetDAO.addDataSetsRepresentationName(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, SAMPLE_REP_NAME_3);
        Set<String> repNames = dataSetDAO.getAllRepresentationsNamesForDataSet(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID);
        Assert.assertTrue(repNames.size() == 3);
        //
        dataSetDAO.removeAllRepresentationsNamesForDataSet(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID);
        repNames = dataSetDAO.getAllRepresentationsNamesForDataSet(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID);
        //
        Assert.assertTrue(repNames.size() == 0);
    }


    @Test
    public void shouldRemoveCountFromAssignmentBucketsWhenRemovingAssignments() {
        // given
        DataSet dataSet = new DataSet();
        dataSet.setId("sampleDataSetID");
        dataSet.setProviderId("sampleProvider");

        // when
        List<Representation> assigned = prepareAssignedRepresentations(dataSet);
        Bucket bucket = dataSetDAO.getCurrentDataSetAssignmentBucket(dataSet.getProviderId(), dataSet.getId());

        // then
        assertNotNull(bucket);
        assertThat(bucket.getRowsCount(), is(Long.valueOf(ASSIGNMENTS_COUNT % MAX_DATASET_ASSIGNMENTS_BUCKET_COUNT)));

        // when
        removeAssignments(dataSet, assigned);
        bucket = dataSetDAO.getCurrentDataSetAssignmentBucket(dataSet.getProviderId(), dataSet.getId());

        // then
        assertNull(bucket);
    }

    private void removeAssignments(DataSet dataSet, List<Representation> assigned) {
        for (Representation representation : assigned) {
            dataSetDAO.removeAssignment(dataSet.getProviderId(), dataSet.getId(), representation.getCloudId(), representation.getRepresentationName(), representation.getVersion());
        }
    }

    private List<Representation> prepareAssignedRepresentations(DataSet dataSet) {
        List<Representation> assigned = new ArrayList<>();
        for (int i = 0; i < ASSIGNMENTS_COUNT; i++) {
            Representation representation = new Representation();
            representation.setCloudId("cloud_id_" + i);
            representation.setDataProvider(dataSet.getProviderId());
            representation.setRepresentationName("representation_" + i);
            representation.setVersion(UUIDs.timeBased().toString());
            dataSetDAO.addAssignment(dataSet.getProviderId(), dataSet.getId(), representation.getCloudId(), representation.getRepresentationName(), representation.getVersion());
            assigned.add(representation);
        }
        return assigned;
    }
}
