package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
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
import static org.junit.Assert.assertThat;


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
    private static final String SAMPLE_REVISION_NAME = "Revision_1";
    private static final String SAMPLE_REVISION_NAME2 = "Revision_2";
    private static final String SAMPLE_REVISION_PROVIDER = "Revision_Provider_1";
    private static final String SAMPLE_REVISION_PROVIDER2 = "Revision_Provider_2";
    private static final String SAMPLE_CLOUD_ID = "Cloud_1";
    private static final String SAMPLE_CLOUD_ID2 = "Cloud_2";
    private static final String SAMPLE_CLOUD_ID3 = "Cloud_3";
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

    @Test
    public void shouldListAllCloudIdForGivenRevisionAndDataset(){
        //given
        Revision revision1 = new Revision(SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME);
        Revision revision2 = new Revision(SAMPLE_REVISION_PROVIDER2, SAMPLE_REVISION_NAME2);
        dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, revision1, SAMPLE_REP_NAME_1,SAMPLE_CLOUD_ID);
        dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, revision1, SAMPLE_REP_NAME_1,SAMPLE_CLOUD_ID2);
        //assigned to different revision
        dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, revision2, SAMPLE_REP_NAME_1,SAMPLE_CLOUD_ID3);

        //when
        List<Properties> cloudIds = dataSetDAO.getDataSetsRevisions(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME, revision1.getCreationTimeStamp(), SAMPLE_REP_NAME_1, null, 2);

        //then
        assertThat(cloudIds.size(),is(2));
        List<String> ids = new ArrayList<>();
        ids.add(cloudIds.get(0).getProperty("cloudId"));
        ids.add(cloudIds.get(1).getProperty("cloudId"));

        assertThat(ids,hasItems(SAMPLE_CLOUD_ID, SAMPLE_CLOUD_ID2));
        assertThat(ids, not(hasItems(SAMPLE_CLOUD_ID3)));
    }

    @Test
    public void shouldListAllCloudIdForGivenRevisionAndDatasetWithLimit(){
        //given
        Revision revision1 = new Revision(SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME);
        Revision revision2 = new Revision(SAMPLE_REVISION_PROVIDER2, SAMPLE_REVISION_NAME2);
        dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, revision1, SAMPLE_REP_NAME_1,SAMPLE_CLOUD_ID);
        dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, revision1, SAMPLE_REP_NAME_1,SAMPLE_CLOUD_ID2);
        //assigned to different revision
        dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, revision2, SAMPLE_REP_NAME_1,SAMPLE_CLOUD_ID3);

        //when
        List<Properties> cloudIds = dataSetDAO.getDataSetsRevisions(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME, revision1.getCreationTimeStamp(), SAMPLE_REP_NAME_1, null, 1);

        //then
        assertThat(cloudIds.size(), is(1));
        List<String> ids = new ArrayList<>();
        ids.add(cloudIds.get(0).getProperty("cloudId"));

        assertThat(ids,hasItems(SAMPLE_CLOUD_ID));
        assertThat(ids, not(hasItems(SAMPLE_CLOUD_ID2,SAMPLE_CLOUD_ID3)));
    }

    @Test
    public void shouldListAllCloudIdForGivenRevisionAndDatasetWithPagination(){
        //given
        Revision revision1 = new Revision(SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME);
        dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, revision1, SAMPLE_REP_NAME_1,SAMPLE_CLOUD_ID);
        dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, revision1, SAMPLE_REP_NAME_1,SAMPLE_CLOUD_ID2);
        dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, revision1, SAMPLE_REP_NAME_1,SAMPLE_CLOUD_ID3);

        String startFrom;

        //when
        List<Properties> cloudIds = dataSetDAO.getDataSetsRevisions(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME, revision1.getCreationTimeStamp(), SAMPLE_REP_NAME_1, null, 1);

        //then
        assertThat(cloudIds.size(), is(2));
        assertThat(cloudIds.get(0).getProperty("cloudId"), is(SAMPLE_CLOUD_ID));
        startFrom = cloudIds.get(1).getProperty("nextSlice");
        Assert.assertTrue(startFrom != null);

        cloudIds = dataSetDAO.getDataSetsRevisions(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME, revision1.getCreationTimeStamp(), SAMPLE_REP_NAME_1, startFrom, 1);
        assertThat(cloudIds.size(), is(2));
        assertThat(cloudIds.get(0).getProperty("cloudId"), is(SAMPLE_CLOUD_ID2));
        startFrom = cloudIds.get(1).getProperty("nextSlice");
        Assert.assertTrue(startFrom != null);

        cloudIds = dataSetDAO.getDataSetsRevisions(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME, revision1.getCreationTimeStamp(), SAMPLE_REP_NAME_1, startFrom, 1);
        assertThat(cloudIds.size(), is(1));
        assertThat(cloudIds.get(0).getProperty("cloudId"), is(SAMPLE_CLOUD_ID3));
    }

    @Test
    public void shouldListAllCloudIdForGivenRevisionForSecondRevision(){
        //given
        Revision revision1 = new Revision(SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME);
        Revision revision2 = new Revision(SAMPLE_REVISION_PROVIDER2, SAMPLE_REVISION_NAME2);
        dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, revision1, SAMPLE_REP_NAME_1,SAMPLE_CLOUD_ID);
        dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, revision1, SAMPLE_REP_NAME_1,SAMPLE_CLOUD_ID2);
        //assigned to different revision
        dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, revision2, SAMPLE_REP_NAME_1,SAMPLE_CLOUD_ID3);

        //when
        List<Properties> cloudIds = dataSetDAO.getDataSetsRevisions(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, SAMPLE_REVISION_PROVIDER2, SAMPLE_REVISION_NAME2, revision2.getCreationTimeStamp(), SAMPLE_REP_NAME_1, null, 3);

        //then
        assertThat(cloudIds.size(), is(1));
        List<String> ids = new ArrayList<>();
        ids.add(cloudIds.get(0).getProperty("cloudId"));

        assertThat(ids,hasItems(SAMPLE_CLOUD_ID3));
        assertThat(ids, not(hasItems(SAMPLE_CLOUD_ID,SAMPLE_CLOUD_ID2)));
    }

    @Test
    public void shouldRemoveRevisionFromDataSet(){
        //given
        Revision revision1 = new Revision(SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME);
        dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, revision1, SAMPLE_REP_NAME_1,SAMPLE_CLOUD_ID);
        dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, revision1, SAMPLE_REP_NAME_1,SAMPLE_CLOUD_ID2);

        //when
        dataSetDAO.removeDataSetsRevision(SAMPLE_PROVIDER_NAME,SAMPLE_DATASET_ID, revision1,
                SAMPLE_REP_NAME_1,SAMPLE_CLOUD_ID);

        //then
        List<Properties> cloudIds = dataSetDAO.getDataSetsRevisions(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME, revision1.getCreationTimeStamp(), SAMPLE_REP_NAME_1, null, 3);
        assertThat(cloudIds.size(), is(1));
        List<String> ids = new ArrayList<>();
        ids.add(cloudIds.get(0).getProperty("cloudId"));

        assertThat(ids,hasItems(SAMPLE_CLOUD_ID2));
        assertThat(ids, not(hasItems(SAMPLE_CLOUD_ID)));
    }

    @Test
    public void shouldRemoveRevisionFromDataSetSecondRevision(){
        //given
        Revision revision1 = new Revision(SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME);
        dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, revision1, SAMPLE_REP_NAME_1,SAMPLE_CLOUD_ID);
        dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, revision1, SAMPLE_REP_NAME_1,SAMPLE_CLOUD_ID2);

        //when
        dataSetDAO.removeDataSetsRevision(SAMPLE_PROVIDER_NAME,SAMPLE_DATASET_ID, revision1,
                SAMPLE_REP_NAME_1,SAMPLE_CLOUD_ID2);

        //then
        List<Properties> cloudIds = dataSetDAO.getDataSetsRevisions(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME, revision1.getCreationTimeStamp(), SAMPLE_REP_NAME_1, null, 3);
        assertThat(cloudIds.size(), is(1));
        List<String> ids = new ArrayList<>();
        ids.add(cloudIds.get(0).getProperty("cloudId"));

        assertThat(ids,hasItems(SAMPLE_CLOUD_ID));
        assertThat(ids, not(hasItems(SAMPLE_CLOUD_ID2)));
    }

    @Test
    public void shouldRemoveAssigmentsOnRemoveWholeDataSet(){
        //given
        Revision revision1 = new Revision(SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME);
        dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, revision1, SAMPLE_REP_NAME_1,SAMPLE_CLOUD_ID);
        dataSetDAO.addDataSetsRevision(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, revision1, SAMPLE_REP_NAME_1,SAMPLE_CLOUD_ID2);

        //when
        dataSetDAO.deleteDataSet(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID);

        //then
        List<Properties> cloudIds = dataSetDAO.getDataSetsRevisions(SAMPLE_PROVIDER_NAME, SAMPLE_DATASET_ID, SAMPLE_REVISION_PROVIDER, SAMPLE_REVISION_NAME, revision1.getCreationTimeStamp(), SAMPLE_REP_NAME_1, null, 3);
        assertThat(cloudIds.size(), is(0));
    }
}
