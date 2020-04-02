package eu.europeana.cloud.service.mcs.rest;

import com.google.common.collect.ImmutableMap;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.utils.Tags;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.test.CassandraTestRunner;
import org.apache.commons.lang3.time.FastDateFormat;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;

import javax.ws.rs.Path;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import static eu.europeana.cloud.common.web.ParamConstants.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * RevisionResourceTest
 */
@RunWith(CassandraTestRunner.class)
public class RevisionResourceTest extends JerseyTest {

    private RecordService recordService;
    private Representation rep;
    private Revision revision;
    private WebTarget revisionWebTarget;
    private WebTarget revisionWebTargetWithTag;
    private WebTarget removeRevisionWebTarget;
    private WebTarget revisionWebTargetWithMultipleTags;
    private static final String PROVIDER_ID = "providerId";
    private static final String TEST_REVESION_NAME = "revisionName";
    private UISClientHandler uisHandler;
    private DataSetService dataSetService;
    private DataProvider dataProvider;
    private Revision revisionForDataProvider;


    @Before
    public void mockUp() throws Exception {
        ApplicationContext applicationContext = ApplicationContextUtils
                .getApplicationContext();
        recordService = applicationContext.getBean(RecordService.class);
        dataSetService = applicationContext.getBean(DataSetService.class);
        uisHandler = applicationContext.getBean(UISClientHandler.class);
        dataProvider = new DataProvider();
        dataProvider.setId("1");
        Mockito.doReturn(new DataProvider()).when(uisHandler)
                .getProvider("1");
        Mockito.doReturn(true).when(uisHandler)
                .existsCloudId(Mockito.anyString());
        rep = recordService.createRepresentation("1", "1", "1");
        revision = new Revision(TEST_REVESION_NAME, PROVIDER_ID);
        revisionForDataProvider = new Revision(TEST_REVESION_NAME, dataProvider.getId());
        Map<String, Object> revisionPathParams = ImmutableMap
                .<String, Object>of(P_CLOUDID,
                        rep.getCloudId(), P_REPRESENTATIONNAME,
                        rep.getRepresentationName(), P_VER,
                        rep.getVersion());
        revisionWebTarget = target(
                RevisionResource.class.getAnnotation(Path.class).value())
                .resolveTemplates(revisionPathParams);

        Map<String, Object> revisionPathParamsWithTag = ImmutableMap
                .<String, Object>of(P_CLOUDID,
                        rep.getCloudId(), P_REPRESENTATIONNAME,
                        rep.getRepresentationName(), P_VER,
                        rep.getVersion(), P_REVISION_NAME,
                        TEST_REVESION_NAME, P_REVISION_PROVIDER_ID,
                        P_REVISION_PROVIDER_ID);

        String revisionWithTagPath = "records/{" + P_CLOUDID + "}/representations/{"
                + P_REPRESENTATIONNAME + "}/versions/{" + P_VER + "}/revisions/{" + P_REVISION_NAME + "}/revisionProvider/{" + P_REVISION_PROVIDER_ID + "}/tag/{" + P_TAG + "}";
        revisionWebTargetWithTag = target(revisionWithTagPath).resolveTemplates(revisionPathParamsWithTag);
        String revisionPathWithMultipleTags = "records/{" + P_CLOUDID + "}/representations/{"
                + P_REPRESENTATIONNAME + "}/versions/{" + P_VER + "}/revisions/{" + P_REVISION_NAME + "}/revisionProvider/{" + P_REVISION_PROVIDER_ID + "}/tags";
        revisionWebTargetWithMultipleTags = target(revisionPathWithMultipleTags).resolveTemplates(revisionPathParamsWithTag);


        Map<String, Object> removeRevisionPathParams = ImmutableMap
                .<String, Object>of(P_CLOUDID,
                        rep.getCloudId(), P_REPRESENTATIONNAME,
                        rep.getRepresentationName(), P_VER,
                        rep.getVersion(), P_REVISION_NAME,
                        TEST_REVESION_NAME, P_REVISION_PROVIDER_ID,
                        P_REVISION_PROVIDER_ID);
        String removeRevisionPath = "records/{" + P_CLOUDID + "}/representations/{"
                + P_REPRESENTATIONNAME + "}/versions/{" + P_VER + "}/revisions/{" + P_REVISION_NAME + "}/revisionProvider/{" + P_REVISION_PROVIDER_ID + "}";
        removeRevisionWebTarget = target(removeRevisionPath).resolveTemplates(removeRevisionPathParams);


    }

    @After
    public void cleanUp() throws Exception {
        recordService.deleteRepresentation(rep.getCloudId(),
                rep.getRepresentationName());
        reset(recordService);
        reset(dataSetService);
    }

    @Override
    public Application configure() {
        return null; //new JerseyConfig().property("contextConfigLocation", "classpath:spiedPersistentServicesTestContext.xml").registerClasses(RevisionIsNotValidExceptionMapper.class);
    }

    @Override
    protected void configureClient(ClientConfig config) {
        config.register(MultiPartFeature.class);
    }


    @Test
    public void shouldAddRevision() throws Exception {
        Response response = revisionWebTarget.request().accept(MediaType.APPLICATION_JSON).post(Entity.json(revision));
        assertNotNull(response);
        assertEquals(response.getStatus(), 201);
    }


    @Test
    public void shouldReturnMethodNotAllowedWhenAddRevisionWithNullProviderId() throws Exception {
        revision.setRevisionProviderId(null);
        Response response = revisionWebTarget.request().accept(MediaType.APPLICATION_JSON).post(Entity.json(revision));
        assertEquals(response.getStatus(), 405);
    }

    @Test
    public void shouldReturnMethodNotAllowedWhenAddRevisionWithNullRevisionName() throws Exception {
        revision.setRevisionName(null);
        Response response = revisionWebTarget.request().accept(MediaType.APPLICATION_JSON).post(Entity.json(revision));
        assertEquals(response.getStatus(), 405);

    }

    @Test
    public void shouldReturnMethodNotAllowedWhenAddRevisionWithNullCreationDate() throws Exception {
        revision.setCreationTimeStamp(null);
        Response response = revisionWebTarget.request().accept(MediaType.APPLICATION_JSON).post(Entity.json(revision));
        assertEquals(response.getStatus(), 405);
    }

    @Test
    public void shouldAddRevisionWithAcceptedTag() throws Exception {
        Response response = revisionWebTargetWithTag.resolveTemplate(P_TAG, Tags.ACCEPTANCE.getTag()).request().post(null);
        assertNotNull(response);
        assertEquals(response.getStatus(), 201);
    }

    @Test
    public void shouldAddRevisionWithPublishedTag() throws Exception {
        Response response = revisionWebTargetWithTag.resolveTemplate(P_TAG, Tags.PUBLISHED.getTag()).request().post(null);
        assertNotNull(response);
        assertEquals(response.getStatus(), 201);
    }

    @Test
    public void shouldAddRevisionWithDeletedTag() throws Exception {
        Response response = revisionWebTargetWithTag.resolveTemplate(P_TAG, Tags.DELETED.getTag()).request().post(null);
        assertNotNull(response);
        assertEquals(response.getStatus(), 201);
    }

    @Test
    public void ShouldReturnBadRequestWhenAddingRevisionWithUnrecognisedTag() throws Exception {
        Response response = revisionWebTargetWithTag.resolveTemplate(P_TAG, "UNDEFINED").request().post(null);
        assertEquals(response.getStatus(), 400);
    }


    @Test
    public void shouldAddRevisionWithMultipleTags() throws Exception {
        Form tagsForm = new Form();
        tagsForm.param(F_TAGS, Tags.ACCEPTANCE.getTag());
        tagsForm.param(F_TAGS, Tags.DELETED.getTag());
        Response response = revisionWebTargetWithMultipleTags.request().post(Entity.form(tagsForm));
        assertNotNull(response);
        assertEquals(response.getStatus(), 201);
    }

    @Test
    public void shouldAddRevisionWithMultipleTags2() throws Exception {
        Form tagsForm = new Form();
        tagsForm.param(F_TAGS, Tags.ACCEPTANCE.getTag());
        tagsForm.param(F_TAGS, Tags.PUBLISHED.getTag());
        tagsForm.param(F_TAGS, Tags.DELETED.getTag());
        Response response = revisionWebTargetWithMultipleTags.request().post(Entity.form(tagsForm));
        assertNotNull(response);
        assertEquals(response.getStatus(), 201);
    }

    @Test
    public void shouldAddRevisionWithEmptyTags() throws Exception {
        Form tagsForm = new Form();
        Response response = revisionWebTargetWithMultipleTags.request().post(Entity.form(tagsForm));
        assertNotNull(response);
        assertEquals(response.getStatus(), 201);
    }


    @Test
    public void ShouldReturnBadRequestWhenAddingRevisionWithUnexpectedTag() throws Exception {
        Form tagsForm = new Form();
        tagsForm.param(F_TAGS, Tags.ACCEPTANCE.getTag());
        tagsForm.param(F_TAGS, Tags.DELETED.getTag());
        tagsForm.param(F_TAGS, "undefined");
        Response response = revisionWebTargetWithMultipleTags.request().post(Entity.form(tagsForm));
        assertNotNull(response);
        assertEquals(response.getStatus(), 400);
    }

    @Test
    public void shouldProperlyAddRevisionToDataSets() throws Exception {
        //given
        DataSet dataSet = dataSetService.createDataSet(dataProvider.getId(), "dataSetId", "DataSetDescription");
        dataSetService.addAssignment(dataProvider.getId(), dataSet.getId(), rep.getCloudId(), rep.getRepresentationName
                (), rep.getVersion());

        //when
        Response response = revisionWebTarget.request().accept(MediaType.APPLICATION_JSON).post(Entity.json(revisionForDataProvider));

        //then
        assertNotNull(response);
        assertEquals(response.getStatus(), 201);
        verify(dataSetService, times(1)).addDataSetsRevisions(
                dataProvider.getId(),
                dataSet.getId(),
                revisionForDataProvider,
                rep.getRepresentationName(),
                rep.getCloudId());
    }

    @Test
    public void shouldProperlyUpdateAllRevisionDatasetsEntries() throws Exception {
        //given
        DataSet dataSet = dataSetService.createDataSet(dataProvider.getId(), "dataSetId", "DataSetDescription");
        dataSetService.addAssignment(dataProvider.getId(), dataSet.getId(), rep.getCloudId(), rep.getRepresentationName
                (), rep.getVersion());

        //when
        Response response = revisionWebTarget.request().accept(MediaType.APPLICATION_JSON).post(Entity.json(revisionForDataProvider));

        //then
        assertNotNull(response);
        assertEquals(response.getStatus(), 201);
        verify(dataSetService, times(1)).updateAllRevisionDatasetsEntries(
                rep.getCloudId(),
                rep.getRepresentationName(),
                rep.getVersion(),
                revisionForDataProvider);
    }


    @Test
    public void shouldRemoveRevisionSuccessfully() throws Exception {
        // given
        String datasetId = "dataset";
        String providerId = "providerId";
        String FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
        FastDateFormat FORMATTER = FastDateFormat.getInstance(FORMAT, TimeZone.getTimeZone("UTC"));
        Date date = new Date();
        String revisionTimeStamp = FORMATTER.format(date);

        Revision revision = new Revision(TEST_REVESION_NAME, P_REVISION_PROVIDER_ID, date, true, false, false);
        Mockito.when(uisHandler.getProvider(providerId)).thenReturn(new DataProvider(providerId));
        Mockito.when(uisHandler.existsProvider(P_REVISION_PROVIDER_ID)).thenReturn(true);
        Mockito.when(uisHandler.existsCloudId(rep.getCloudId())).thenReturn(true);
        dataSetService.createDataSet(providerId, datasetId, "");
        dataSetService.addAssignment(providerId, datasetId, rep.getCloudId(), rep.getRepresentationName(), rep.getVersion());
        dataSetService.addDataSetsRevisions(providerId, datasetId, revision, rep.getRepresentationName(), rep.getCloudId());

        Response response = removeRevisionWebTarget.queryParam(F_REVISION_TIMESTAMP, revisionTimeStamp).request().delete();

        assertThat(response.getStatus(), is(Response.Status.NO_CONTENT.getStatusCode()));
    }

}