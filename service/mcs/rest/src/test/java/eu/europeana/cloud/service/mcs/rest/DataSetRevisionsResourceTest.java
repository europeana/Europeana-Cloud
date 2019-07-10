package eu.europeana.cloud.service.mcs.rest;


import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraRecordDAO;
import eu.europeana.cloud.test.CassandraTestRunner;
import org.apache.commons.lang3.time.FastDateFormat;
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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import static eu.europeana.cloud.common.web.ParamConstants.*;
import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * DataSetResourceTest
 *
 * @author akrystian
 */
@RunWith(CassandraTestRunner.class)
public class DataSetRevisionsResourceTest extends JerseyTest {

    private DataSetService dataSetService;
    private CassandraRecordDAO cassandraRecordDAO;

    private WebTarget dataSetWebTarget;

    private UISClientHandler uisHandler;

    private SimpleDateFormat dateFormat;

    @Override
    public Application configure() {
        return new JerseyConfig().property("contextConfigLocation", "classpath:spiedPersistentServicesTestContext.xml");
    }

    @Before
    public void mockUp()
            throws Exception {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        uisHandler = applicationContext.getBean(UISClientHandler.class);
        dataSetService = applicationContext.getBean(DataSetService.class);
        cassandraRecordDAO = applicationContext.getBean(CassandraRecordDAO.class);
        dataSetWebTarget = target(DataSetRevisionsResource.class.getAnnotation(Path.class).value());
        dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    }


    @After
    public void cleanUp() throws Exception {
        Mockito.reset(uisHandler);
    }


    @Test
    public void shouldRemoveRevisionSuccessfully() throws Exception {
        // given
        String datasetId = "dataset";
        String providerId = "providerId";
        String revisionName = "revisionName";
        String revisionProviderId = "revisionProviderId";
        String representationName = "representationName";
        String cloudId = "cloudId";
        String FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
        FastDateFormat FORMATTER = FastDateFormat.getInstance(FORMAT, TimeZone.getTimeZone("UTC"));
        Date date = new Date();
        String revisionTimeStamp = FORMATTER.format(date);

        Revision revision = new Revision(revisionName, revisionProviderId, date, true, false, false);
        Mockito.when(uisHandler.getProvider(providerId)).thenReturn(new DataProvider(providerId));
        Mockito.when(uisHandler.existsProvider(providerId)).thenReturn(true);
        Mockito.when(uisHandler.existsCloudId(cloudId)).thenReturn(true);
        dataSetService.createDataSet(providerId, datasetId, "");
        dataSetService.addDataSetsRevisions(providerId, datasetId, revision, representationName, cloudId);

        Representation representation = cassandraRecordDAO.createRepresentation(cloudId, representationName, providerId, new Date());

        WebTarget dataSetWebTarget1 = target(DataSetRevisionsResource.class.getAnnotation(Path.class).value()).path("/records/{" + P_CLOUDID + "}/versions/{" + P_VER + "}");
        // when
        dataSetWebTarget1 = dataSetWebTarget1
                .resolveTemplate(P_PROVIDER, providerId)
                .resolveTemplate(P_DATASET, datasetId)
                .resolveTemplate(P_REPRESENTATIONNAME, representationName)
                .resolveTemplate(P_REVISION_NAME, revisionName)
                .resolveTemplate(P_REVISION_PROVIDER_ID, revisionProviderId)
                .resolveTemplate(P_CLOUDID, cloudId)
                .resolveTemplate(ParamConstants.P_VER, representation.getVersion()).queryParam(F_REVISION_TIMESTAMP, revisionTimeStamp);
        Response response = dataSetWebTarget1.request().delete();
        assertThat(response.getStatus(), is(Response.Status.NO_CONTENT.getStatusCode()));
    }

    @Test
    public void shouldRetrieveCloudIdBelongToRevision() throws Exception {
        // given
        String datasetId = "dataset";
        String providerId = "providerId";
        String revisionName = "revisionName";
        String revisionProviderId = "revisionProviderId";
        String representationName = "representationName";
        String cloudId = "cloudId";
        Revision revision = new Revision(revisionName, revisionProviderId, new Date(), true, false, false);
        Mockito.when(uisHandler.getProvider(providerId)).thenReturn(new DataProvider());
        Mockito.when(uisHandler.existsProvider(providerId)).thenReturn(true);
        dataSetService.createDataSet(providerId, datasetId, "");
        dataSetService.addDataSetsRevisions(providerId, datasetId, revision, representationName, cloudId);

        // when
        dataSetWebTarget = dataSetWebTarget.
                resolveTemplate(P_DATASET, datasetId).
                resolveTemplate(P_PROVIDER, providerId).
                resolveTemplate(P_REVISION_NAME, revisionName).
                resolveTemplate(P_REVISION_PROVIDER_ID, revisionProviderId).
                resolveTemplate(P_REPRESENTATIONNAME, representationName).
                queryParam(F_REVISION_TIMESTAMP, dateFormat.format(revision.getCreationTimeStamp())).
                queryParam(F_LIMIT, 10);
        Response response = dataSetWebTarget.request().get();
        System.out.println(response);

        //then
        assertThat(response.getStatus(), is(Response.Status.OK.getStatusCode()));
        List<CloudTagsResponse> cloudIds = response.readEntity(ResultSlice.class).getResults();
        assertThat(cloudIds.size(), is(1));
        assertThat(cloudIds.get(0).getCloudId(), is(cloudId));
    }

    @Test
    public void shouldGetEmptyResultSetOnRetrievingNonExistingRevision() throws Exception {
        // given
        String datasetId = "dataset";
        String providerId = "providerId";
        String revisionName = "revisionName";
        String revisionName2 = "revisionName2";
        String revisionProviderId = "revisionProviderId";
        String representationName = "representationName";
        Revision revision = new Revision(revisionName, revisionProviderId, new Date(), true, false, false);
        Revision revision2 = new Revision(revisionName2, revisionProviderId, new Date(), true, false, false);
        String cloudId = "cloudId";
        Mockito.when(uisHandler.getProvider(providerId)).thenReturn(new DataProvider());
        Mockito.when(uisHandler.existsProvider(providerId)).thenReturn(true);
        dataSetService.createDataSet(providerId, datasetId, "");
        dataSetService.addDataSetsRevisions(providerId, datasetId, revision, representationName, cloudId);

        // when
        dataSetWebTarget = dataSetWebTarget.
                resolveTemplate(P_DATASET, datasetId).
                resolveTemplate(P_PROVIDER, providerId).
                resolveTemplate(P_CLOUDID, cloudId).
                resolveTemplate(P_REVISION_NAME, revisionName2).
                resolveTemplate(P_REVISION_PROVIDER_ID, revisionProviderId).
                resolveTemplate(P_REPRESENTATIONNAME, representationName).
                queryParam(F_REVISION_TIMESTAMP, dateFormat.format(revision2.getCreationTimeStamp())).
                queryParam(F_LIMIT, 1);
        Response response = dataSetWebTarget.request().get();

        //then
        assertThat(response.getStatus(), is(Response.Status.OK.getStatusCode()));
        List<CloudTagsResponse> cloudIds = response.readEntity(ResultSlice.class).getResults();
        assertThat(cloudIds.size(), is(0));
    }

    @Test
    public void shouldGetEmptyResultSetOnRetrievingNonExistingRevision2() throws Exception {
        // given
        String datasetId = "dataset";
        String providerId = "providerId";
        String revisionName = "revisionName";
        String revisionProviderId = "revisionProviderId";
        String representationName = "representationName";
        String revisionTimestamp = "2016-12-02T01:08:28.059";
        Mockito.when(uisHandler.getProvider(providerId)).thenReturn(new DataProvider());
        Mockito.when(uisHandler.existsProvider(providerId)).thenReturn(true);
        dataSetService.createDataSet(providerId, datasetId, "");

        // when
        dataSetWebTarget = dataSetWebTarget.
                resolveTemplate(P_DATASET, datasetId).
                resolveTemplate(P_PROVIDER, providerId).
                resolveTemplate(P_REVISION_NAME, revisionName).
                resolveTemplate(P_REVISION_PROVIDER_ID, revisionProviderId).
                resolveTemplate(P_REPRESENTATIONNAME, representationName).
                queryParam(F_REVISION_TIMESTAMP, revisionTimestamp).
                queryParam(F_LIMIT, 10);
        Response response = dataSetWebTarget.request().get();

        //then
        assertThat(response.getStatus(), is(Response.Status.OK.getStatusCode()));
        List<CloudTagsResponse> cloudIds = response.readEntity(ResultSlice.class).getResults();
        assertThat(cloudIds.size(), is(0));
    }


    @Test
    public void shouldResultWithNotDefinedStart() throws Exception {
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
        Mockito.when(uisHandler.existsProvider(providerId)).thenReturn(true);
        dataSetService.createDataSet(providerId, datasetId, "");
        dataSetService.addDataSetsRevisions(providerId, datasetId, revision, representationName, cloudId);
        dataSetService.addDataSetsRevisions(providerId, datasetId, revision, representationName, cloudId2);
        dataSetService.addDataSetsRevisions(providerId, datasetId, revision, representationName, cloudId3);

        // when
        dataSetWebTarget = dataSetWebTarget.
                resolveTemplate(P_PROVIDER, providerId).
                resolveTemplate(P_DATASET, datasetId).
                resolveTemplate(P_REPRESENTATIONNAME, representationName).
                resolveTemplate(P_REVISION_NAME, revisionName).
                resolveTemplate(P_REVISION_PROVIDER_ID, revisionProviderId).
                queryParam(F_REVISION_TIMESTAMP, dateFormat.format(revision.getCreationTimeStamp())).
                queryParam(F_LIMIT, 1);
        Response response = dataSetWebTarget.request().get();

        //then
        assertThat(response.getStatus(), is(Response.Status.OK.getStatusCode()));
        List<CloudTagsResponse> cloudIds = response.readEntity(ResultSlice.class).getResults();
        assertThat(cloudIds.size(), is(1));
        assertThat(cloudIds.get(0).getCloudId(), is(cloudId));
    }

    @Test
    public void shouldRetrieveCloudIdsPageByPage() throws Exception {
        // given
        String datasetId = "dataset";
        String providerId = "providerId";
        String representationName = "representationName";
        String revisionName = "revisionName";
        String revisionProviderId = "revisionProviderId";

        Mockito.when(uisHandler.getProvider(providerId)).thenReturn(new DataProvider());
        Mockito.when(uisHandler.existsProvider(providerId)).thenReturn(true);
        dataSetService.createDataSet(providerId, datasetId, "");

        Revision revision = new Revision(revisionName, revisionProviderId, new Date(), true, false, false);
        String cloudId = "cloudId";
        String cloudId2 = "cloudId2";
        String cloudId3 = "cloudId3";
        dataSetService.addDataSetsRevisions(providerId, datasetId, revision, representationName, cloudId);
        dataSetService.addDataSetsRevisions(providerId, datasetId, revision, representationName, cloudId2);
        dataSetService.addDataSetsRevisions(providerId, datasetId, revision, representationName, cloudId3);

        // when get first page (set page size to 1)
        WebTarget target = dataSetWebTarget.
                resolveTemplate(P_PROVIDER, providerId).
                resolveTemplate(P_DATASET, datasetId).
                resolveTemplate(P_REPRESENTATIONNAME, representationName).
                resolveTemplate(P_REVISION_NAME, revisionName).
                resolveTemplate(P_REVISION_PROVIDER_ID, revisionProviderId).
                queryParam(F_REVISION_TIMESTAMP, dateFormat.format(revision.getCreationTimeStamp())).
                queryParam(F_LIMIT, 1);
        Response response = target.request().get();

        // then
        assertThat(response.getStatus(), is(Response.Status.OK.getStatusCode()));
        ResultSlice<CloudTagsResponse> slice = response.readEntity(ResultSlice.class);
        List<CloudTagsResponse> cloudIds = slice.getResults();
        assertThat(cloudIds.size(), is(1));
        assertThat(cloudIds.get(0).getCloudId(), is(cloudId));
        assertNotNull(slice.getNextSlice());

        // when get second page
        target = dataSetWebTarget.
                resolveTemplate(P_PROVIDER, providerId).
                resolveTemplate(P_DATASET, datasetId).
                resolveTemplate(P_REPRESENTATIONNAME, representationName).
                resolveTemplate(P_REVISION_NAME, revisionName).
                resolveTemplate(P_REVISION_PROVIDER_ID, revisionProviderId).
                queryParam(F_REVISION_TIMESTAMP, dateFormat.format(revision.getCreationTimeStamp())).
                queryParam(F_START_FROM, slice.getNextSlice()).
                queryParam(F_LIMIT, 1);
        response = target.request().get();

        // then
        assertThat(response.getStatus(), is(Response.Status.OK.getStatusCode()));
        slice = response.readEntity(ResultSlice.class);
        cloudIds = slice.getResults();
        assertThat(cloudIds.size(), is(1));
        assertThat(cloudIds.get(0).getCloudId(), is(cloudId2));
        assertNotNull(slice.getNextSlice());

        // when get last page
        target = dataSetWebTarget.
                resolveTemplate(P_PROVIDER, providerId).
                resolveTemplate(P_DATASET, datasetId).
                resolveTemplate(P_REPRESENTATIONNAME, representationName).
                resolveTemplate(P_REVISION_NAME, revisionName).
                resolveTemplate(P_REVISION_PROVIDER_ID, revisionProviderId).
                queryParam(F_REVISION_TIMESTAMP, dateFormat.format(revision.getCreationTimeStamp())).
                queryParam(F_START_FROM, slice.getNextSlice()).
                queryParam(F_LIMIT, 1);
        response = target.request().get();

        // then
        assertThat(response.getStatus(), is(Response.Status.OK.getStatusCode()));
        slice = response.readEntity(ResultSlice.class);
        cloudIds = slice.getResults();
        assertThat(cloudIds.size(), is(1));
        assertThat(cloudIds.get(0).getCloudId(), is(cloudId3));
        assertNull(slice.getNextSlice());
    }
}