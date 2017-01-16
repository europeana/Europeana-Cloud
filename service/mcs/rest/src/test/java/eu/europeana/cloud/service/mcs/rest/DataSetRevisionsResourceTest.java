package eu.europeana.cloud.service.mcs.rest;


import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.utils.RevisionUtils;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.test.CassandraTestRunner;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;

import javax.ws.rs.Path;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import java.util.Date;
import java.util.List;

import static eu.europeana.cloud.common.web.ParamConstants.*;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;

/**
 * DataSetResourceTest
 *
 * @author akrystian
 */
@RunWith(CassandraTestRunner.class)
public class DataSetRevisionsResourceTest extends JerseyTest{

    private DataSetService dataSetService;

    private WebTarget dataSetWebTarget;

    private UISClientHandler uisHandler;

    @Override
    public Application configure(){
        return new JerseyConfig().property("contextConfigLocation", "classpath:spiedPersistentServicesTestContext.xml");
    }

    @Before
    public void mockUp()
            throws Exception{
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        uisHandler = applicationContext.getBean(UISClientHandler.class);
        dataSetService = applicationContext.getBean(DataSetService.class);

        dataSetWebTarget = target(DataSetRevisionsResource.class.getAnnotation(Path.class).value());
    }


    @After
    public void cleanUp() throws Exception{
        Mockito.reset(uisHandler);
    }

    @Test
    public void shouldRetrieveCloudIdBelongToRevision() throws Exception{
        // given
        String datasetId = "dataset";
        String providerId = "providerId";
        String revisionName = "revisionName";
        String revisionProviderId = "revisionProviderId";
        String representationName = "representationName";
        String revisionTimestamp = "2016-12-02T01:08:28.059";
        String cloudId = "cloudId";
        Revision revision = new Revision(revisionName, revisionProviderId, new Date(), true, false, false);
        Mockito.when(uisHandler.getProvider(providerId)).thenReturn(new DataProvider());

        dataSetService.addDataSetsRevisions(providerId, datasetId, revision, representationName, cloudId);

        // when
        dataSetWebTarget = dataSetWebTarget.
                resolveTemplate(P_DATASET, datasetId).
                resolveTemplate(P_PROVIDER, providerId).
                resolveTemplate(P_CLOUDID, cloudId).
                resolveTemplate(P_REVISION_NAME, revisionName).
                resolveTemplate(P_REVISION_PROVIDER_ID, revisionProviderId).
                resolveTemplate(P_REPRESENTATIONNAME, representationName).
                queryParam(F_REVISION_TIMESTAMP, revision.getCreationTimeStamp().toString()).
                queryParam(F_START_FROM, null);
        Response response = dataSetWebTarget.request().get();

        //then
        assertThat(Response.Status.OK.getStatusCode(),is(response.getStatus()));
        List<String> acltualCloudIds = response.readEntity(ResultSlice.class).getResults();
        assertThat(acltualCloudIds,hasItem(cloudId));
    }

    @Test
    public void shouldGetEmptyResultSetOnRetrievingNonExistingRevision() throws Exception{
        // given
        String datasetId = "dataset";
        String providerId = "providerId";
        String revisionName = "revisionName";
        String revisionProviderId = "revisionProviderId";
        String representationName = "representationName";
        String cloudId = "cloudId";
        Mockito.when(uisHandler.getProvider(providerId)).thenReturn(new DataProvider());

        // when
        dataSetWebTarget = dataSetWebTarget.
                resolveTemplate(P_DATASET, datasetId).
                resolveTemplate(P_PROVIDER, providerId).
                resolveTemplate(P_CLOUDID, cloudId).
                resolveTemplate(P_REVISION_NAME, revisionName).
                resolveTemplate(P_REVISION_PROVIDER_ID, revisionProviderId).
                resolveTemplate(P_REPRESENTATIONNAME, representationName).
                queryParam(F_START_FROM, null);
        Response response = dataSetWebTarget.request().get();

        //then
        assertThat(Response.Status.OK.getStatusCode(),is(response.getStatus()));
        List<String> acltualCloudIds = response.readEntity(ResultSlice.class).getResults();
        assertThat(acltualCloudIds,not(hasItem(cloudId)));
    }

    @Test
    public void shouldGetEmptyResultSetOnRetrievingNonExistingRevision2() throws Exception{
        // given
        String datasetId = "dataset";
        String providerId = "providerId";
        String revisionName = "revisionName";
        String revisionProviderId = "revisionProviderId";
        String representationName = "representationName";
        String cloudId = "cloudId";
        Mockito.when(uisHandler.getProvider(providerId)).thenReturn(new DataProvider());

        // when
        dataSetWebTarget = dataSetWebTarget.
                resolveTemplate(P_DATASET, datasetId).
                resolveTemplate(P_PROVIDER, providerId).
                resolveTemplate(P_CLOUDID, cloudId).
                resolveTemplate(P_REVISION_NAME, revisionName).
                resolveTemplate(P_REVISION_PROVIDER_ID, revisionProviderId).
                resolveTemplate(P_REPRESENTATIONNAME, representationName).
                queryParam(F_START_FROM,cloudId);
        Response response = dataSetWebTarget.request().get();

        //then
        assertThat(Response.Status.OK.getStatusCode(),is(response.getStatus()));
        List<String> acltualCloudIds = response.readEntity(ResultSlice.class).getResults();
        assertThat(acltualCloudIds,not(hasItem(cloudId)));
    }

    @Test
    public void shouldResultWithNotDefinedStart() throws Exception{
        // given
        String datasetId = "dataset";
        String providerId = "providerId";
        String representationName = "representationName";
        String revisionName = "revisionName";
        String revisionProviderId = "revisionProviderId";
        Revision revision = new Revision(revisionName, revisionProviderId, new Date(), true, false, false);
        String cloudId = "cloudId";
        String cloudId2 = "cloudId2";
        String cloudId3 = "cloudId3";
        Mockito.when(uisHandler.getProvider(providerId)).thenReturn(new DataProvider());
        dataSetService.addDataSetsRevisions(providerId, datasetId, revision, representationName, cloudId);
        dataSetService.addDataSetsRevisions(providerId, datasetId, revision, representationName, cloudId2);
        dataSetService.addDataSetsRevisions(providerId, datasetId, revision, representationName, cloudId3);

        // when
        dataSetWebTarget = dataSetWebTarget.
                resolveTemplate(P_DATASET, datasetId).
                resolveTemplate(P_PROVIDER, providerId).
                resolveTemplate(P_CLOUDID, cloudId).
                resolveTemplate(P_REPRESENTATIONNAME, representationName).
                resolveTemplate(P_REVISION_NAME, revisionName).
                resolveTemplate(P_REVISION_PROVIDER_ID, revisionProviderId).
                queryParam(F_REVISION_TIMESTAMP, revision.getCreationTimeStamp().toString()).
                queryParam(F_LIMIT, 1);
        Response response = dataSetWebTarget.request().get();

        //then
        assertThat(Response.Status.OK.getStatusCode(),is(response.getStatus()));
        List<String> acltualCloudIds = response.readEntity(ResultSlice.class).getResults();
        assertThat(acltualCloudIds,hasItem(cloudId));
        assertThat(acltualCloudIds,not(hasItem(cloudId2)));
        assertThat(acltualCloudIds,not(hasItem(cloudId3)));
    }
}