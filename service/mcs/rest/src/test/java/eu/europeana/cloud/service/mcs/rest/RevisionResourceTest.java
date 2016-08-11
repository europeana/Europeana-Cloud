package eu.europeana.cloud.service.mcs.rest;

import com.google.common.collect.ImmutableMap;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.utils.Tags;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.RevisionIsNotValidExceptionMapper;
import eu.europeana.cloud.test.CassandraTestRunner;
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
import java.util.Map;

import static eu.europeana.cloud.common.web.ParamConstants.*;
import static eu.europeana.cloud.common.web.ParamConstants.TAG;
import static org.junit.Assert.*;


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
    private WebTarget revisionWebTargetWithMultipleTags;
    private static final String PROVIDER_ID = "providerId";
    private static final String TEST_REVESION_NAME = "revisionName";
    private UISClientHandler uisHandler;

    @Before
    public void mockUp() throws Exception {
        ApplicationContext applicationContext = ApplicationContextUtils
                .getApplicationContext();

        recordService = applicationContext.getBean(RecordService.class);
        uisHandler = applicationContext.getBean(UISClientHandler.class);
        DataProvider dataProvider = new DataProvider();
        dataProvider.setId("1");
        Mockito.doReturn(new DataProvider()).when(uisHandler)
                .getProvider("1");
        Mockito.doReturn(true).when(uisHandler)
                .existsCloudId(Mockito.anyString());
        rep = recordService.createRepresentation("1", "1", "1");
        revision = new Revision(TEST_REVESION_NAME, PROVIDER_ID);
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
                        rep.getVersion(), REVISION_NAME,
                        TEST_REVESION_NAME, REVISION_PROVIDER_ID,
                        REVISION_PROVIDER_ID);
        String revisionWithTagPath = "records/{" + P_CLOUDID + "}/representations/{"
                + P_REPRESENTATIONNAME + "}/versions/{" + P_VER + "}/revisions/{" + REVISION_NAME + "}/revisionProvider/{" + REVISION_PROVIDER_ID + "}/tag/{" + TAG + "}";
        revisionWebTargetWithTag = target(revisionWithTagPath).resolveTemplates(revisionPathParamsWithTag);
        String revisionPathWithMultipleTags = "records/{" + P_CLOUDID + "}/representations/{"
                + P_REPRESENTATIONNAME + "}/versions/{" + P_VER + "}/revisions/{" + REVISION_NAME + "}/revisionProvider/{" + REVISION_PROVIDER_ID + "}/tags";
        revisionWebTargetWithMultipleTags = target(revisionPathWithMultipleTags).resolveTemplates(revisionPathParamsWithTag);


    }

    @After
    public void cleanUp() throws Exception {
        recordService.deleteRepresentation(rep.getCloudId(),
                rep.getRepresentationName());
    }

    @Override
    public Application configure() {
        return new JerseyConfig().property("contextConfigLocation",
                "classpath:spiedPersistentServicesTestContext.xml")
                .registerClasses(RevisionIsNotValidExceptionMapper.class);
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
    public void shouldReturnMethodNotAllowedWhenAddRevisionWithNullUpdateDate() throws Exception {
        revision.setUpdateTimeStamp(null);
        Response response = revisionWebTarget.request().accept(MediaType.APPLICATION_JSON).post(Entity.json(revision));
        assertEquals(response.getStatus(), 405);

    }


    @Test
    public void shouldAddRevisionWithAcceptedTag() throws Exception {
        Response response = revisionWebTargetWithTag.resolveTemplate(TAG, Tags.ACCEPTANCE.getTag()).request().post(null);
        assertNotNull(response);
        assertEquals(response.getStatus(), 201);
    }

    @Test
    public void shouldAddRevisionWithPublishedTag() throws Exception {
        Response response = revisionWebTargetWithTag.resolveTemplate(TAG, Tags.PUBLISHED.getTag()).request().post(null);
        assertNotNull(response);
        assertEquals(response.getStatus(), 201);
    }

    @Test
    public void shouldAddRevisionWithDeletedTag() throws Exception {
        Response response = revisionWebTargetWithTag.resolveTemplate(TAG, Tags.DELETED.getTag()).request().post(null);
        assertNotNull(response);
        assertEquals(response.getStatus(), 201);
    }

    @Test
    public void ShouldReturnBadRequestWhenAddingRevisionWithUnrecognisedTag() throws Exception {
        Response response = revisionWebTargetWithTag.resolveTemplate(TAG, "UNDEFINED").request().post(null);
        assertEquals(response.getStatus(), 400);
    }


    @Test
    public void shouldAddRevisionWithMultipleTags() throws Exception {
        Form tagsForm = new Form();
        tagsForm.param(TAGS, Tags.ACCEPTANCE.getTag());
        tagsForm.param(TAGS, Tags.DELETED.getTag());
        Response response = revisionWebTargetWithMultipleTags.request().post(Entity.form(tagsForm));
        assertNotNull(response);
        assertEquals(response.getStatus(), 201);
    }

    @Test
    public void shouldAddRevisionWithMultipleTags2() throws Exception {
        Form tagsForm = new Form();
        tagsForm.param(TAGS, Tags.ACCEPTANCE.getTag());
        tagsForm.param(TAGS, Tags.PUBLISHED.getTag());
        tagsForm.param(TAGS, Tags.DELETED.getTag());
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
        tagsForm.param(TAGS, Tags.ACCEPTANCE.getTag());
        tagsForm.param(TAGS, Tags.DELETED.getTag());
        tagsForm.param(TAGS, "undefined");
        Response response = revisionWebTargetWithMultipleTags.request().post(Entity.form(tagsForm));
        assertNotNull(response);
        assertEquals(response.getStatus(), 400);
    }


}