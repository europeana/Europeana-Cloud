package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Set;
import java.util.UUID;

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
    private static final UUID SAMPLE_VERSION_ID = TimeUUIDUtils.getTimeUUID(new java.util.Date().getTime());

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
    public void shouldListAllRepresentationsNamesForGivenDataSet() {
        dataSetDAO.addDataSetsRepresentationName(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, SAMPLE_REP_NAME_1);
        dataSetDAO.addDataSetsRepresentationName(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, SAMPLE_REP_NAME_3);
        Set<String> representations = dataSetDAO.getAllRepresentationsNamesForDataSet(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID);
        Assert.assertTrue(representations.contains(SAMPLE_REP_NAME_1));
        Assert.assertFalse(representations.contains(SAMPLE_REP_NAME_2));
        Assert.assertTrue(representations.contains(SAMPLE_REP_NAME_3));
    }
}
