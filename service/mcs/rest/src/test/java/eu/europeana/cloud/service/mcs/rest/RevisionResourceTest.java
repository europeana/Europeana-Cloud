package eu.europeana.cloud.service.mcs.rest;

import com.google.common.collect.ImmutableMap;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.utils.Tags;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.RestInterfaceConstants;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.test.CassandraTestRunner;
import org.apache.commons.lang3.time.FastDateFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.web.util.UriComponentsBuilder;

import javax.ws.rs.core.MediaType;
import java.net.URI;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import static eu.europeana.cloud.common.web.ParamConstants.CLOUD_ID;
import static eu.europeana.cloud.common.web.ParamConstants.F_REVISION_TIMESTAMP;
import static eu.europeana.cloud.common.web.ParamConstants.F_TAGS;
import static eu.europeana.cloud.common.web.ParamConstants.REPRESENTATION_NAME;
import static eu.europeana.cloud.common.web.ParamConstants.REVISION_NAME;
import static eu.europeana.cloud.common.web.ParamConstants.REVISION_PROVIDER_ID;
import static eu.europeana.cloud.common.web.ParamConstants.TAG;
import static eu.europeana.cloud.common.web.ParamConstants.VERSION;
import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.toJson;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


/**
 * RevisionResourceTest
 */
@RunWith(CassandraTestRunner.class)
public class RevisionResourceTest extends CassandraBasedAbstractResourceTest {

    private RecordService recordService;
    private Representation rep;
    private Revision revision;
    private URI revisionWebTarget;
    private String revisionWebTargetWithTag;
    private URI removeRevisionWebTarget;
    private URI revisionWebTargetWithMultipleTags;
    private static final String PROVIDER_ID = "providerId";
    private static final String TEST_REVISION_NAME = "revisionName";
    private UISClientHandler uisHandler;
    private DataSetService dataSetService;
    private DataProvider dataProvider;
    private Revision revisionForDataProvider;


    @Before
    public void mockUp() throws Exception {
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
        revision = new Revision(TEST_REVISION_NAME, PROVIDER_ID);
        revisionForDataProvider = new Revision(TEST_REVISION_NAME, dataProvider.getId());
        Map<String, Object> revisionPathParams = ImmutableMap
                .of(CLOUD_ID, rep.getCloudId(),
                        REPRESENTATION_NAME, rep.getRepresentationName(),
                        VERSION, rep.getVersion());
        revisionWebTarget = UriComponentsBuilder.fromUriString(
                RestInterfaceConstants.REVISION_ADD).build(revisionPathParams);

        Map<String, Object> revisionPathParamsWithTag = ImmutableMap
                .of(
                        CLOUD_ID, rep.getCloudId(),
                        REPRESENTATION_NAME, rep.getRepresentationName(),
                        VERSION, rep.getVersion(),
                        REVISION_NAME, TEST_REVISION_NAME,
                        REVISION_PROVIDER_ID, REVISION_PROVIDER_ID);

        String revisionWithTagPath = "/records/{" + CLOUD_ID + "}/representations/{"
                + REPRESENTATION_NAME + "}/versions/{" + VERSION + "}/revisions/{" + REVISION_NAME + "}/revisionProvider/{" + REVISION_PROVIDER_ID + "}/tag/";
        revisionWebTargetWithTag = UriComponentsBuilder.fromUriString(revisionWithTagPath).build(revisionPathParamsWithTag).toString()+"{" + TAG + "}";
        String revisionPathWithMultipleTags = "/records/{" + CLOUD_ID + "}/representations/{"
                + REPRESENTATION_NAME + "}/versions/{" + VERSION + "}/revisions/{" + REVISION_NAME + "}/revisionProvider/{" + REVISION_PROVIDER_ID + "}/tags";
        revisionWebTargetWithMultipleTags = UriComponentsBuilder.fromUriString(revisionPathWithMultipleTags).build(revisionPathParamsWithTag);


        Map<String, Object> removeRevisionPathParams = ImmutableMap
                .of(CLOUD_ID, rep.getCloudId(),
                        REPRESENTATION_NAME, rep.getRepresentationName(),
                        VERSION, rep.getVersion(),
                        REVISION_NAME, TEST_REVISION_NAME,
                        REVISION_PROVIDER_ID, REVISION_PROVIDER_ID);
        String removeRevisionPath = "/records/{" + CLOUD_ID + "}/representations/{"
                + REPRESENTATION_NAME + "}/versions/{" + VERSION + "}/revisions/{" + REVISION_NAME + "}/revisionProvider/{" + REVISION_PROVIDER_ID + "}";
        removeRevisionWebTarget = UriComponentsBuilder.fromUriString(removeRevisionPath).build(removeRevisionPathParams);


    }

    @After
    public void cleanUp() throws Exception {
        recordService.deleteRepresentation(rep.getCloudId(),
                rep.getRepresentationName());
        reset(recordService);
        reset(dataSetService);
    }

    @Test
    public void shouldAddRevision() throws Exception {
        mockMvc.perform(post(revisionWebTarget)
                .contentType(MediaType.APPLICATION_JSON).content(toJson(revision)))
                .andExpect(status().isCreated());
    }


    @Test
    public void shouldReturnMethodNotAllowedWhenAddRevisionWithNullProviderId() throws Exception {
        revision.setRevisionProviderId(null);
        mockMvc.perform(post(revisionWebTarget).contentType(MediaType.APPLICATION_JSON).content(toJson(revision)))
                .andExpect(status().isMethodNotAllowed());
    }

    @Test
    public void shouldReturnMethodNotAllowedWhenAddRevisionWithNullRevisionName() throws Exception {
        revision.setRevisionName(null);
        mockMvc.perform(post(revisionWebTarget).contentType(MediaType.APPLICATION_JSON).content(toJson(revision)))
                .andExpect(status().isMethodNotAllowed());
    }

    @Test
    public void shouldReturnMethodNotAllowedWhenAddRevisionWithNullCreationDate() throws Exception {
        revision.setCreationTimeStamp(null);
        mockMvc.perform(post(revisionWebTarget).contentType(MediaType.APPLICATION_JSON).content(toJson(revision)))
                .andExpect(status().isMethodNotAllowed());

    }

    @Test
    public void shouldAddRevisionWithAcceptedTag() throws Exception {
        mockMvc.perform(post(revisionWebTargetWithTag, Tags.ACCEPTANCE.getTag()))
                .andExpect(status().isCreated());
    }

    @Test
    public void shouldAddRevisionWithPublishedTag() throws Exception {
        mockMvc.perform(post(revisionWebTargetWithTag, Tags.PUBLISHED.getTag()))
                .andExpect(status().isCreated());
    }

    @Test
    public void shouldAddRevisionWithDeletedTag() throws Exception {
        mockMvc.perform(post(revisionWebTargetWithTag, Tags.DELETED.getTag()))
                .andExpect(status().isCreated());
    }

    @Test
    public void ShouldReturnBadRequestWhenAddingRevisionWithUnrecognisedTag() throws Exception {
        mockMvc.perform(post(revisionWebTargetWithTag, "UNDEFINED"))
                .andExpect(status().isBadRequest());
    }


    @Test
    public void shouldAddRevisionWithMultipleTags() throws Exception {
        mockMvc.perform(post(revisionWebTargetWithMultipleTags)
                .contentType(org.springframework.http.MediaType.APPLICATION_FORM_URLENCODED)
                .param(F_TAGS, Tags.ACCEPTANCE.getTag(), Tags.DELETED.getTag()))
                .andExpect(status().isCreated());
    }

    @Test
    public void shouldAddRevisionWithMultipleTags2() throws Exception {
        mockMvc.perform(post(revisionWebTargetWithMultipleTags)
                .contentType(org.springframework.http.MediaType.APPLICATION_FORM_URLENCODED)
                .param(F_TAGS, Tags.ACCEPTANCE.getTag(), Tags.PUBLISHED.getTag(), Tags.DELETED.getTag()))
                .andExpect(status().isCreated());
    }

    @Test
    public void shouldAddRevisionWithEmptyTags() throws Exception {
        mockMvc.perform(post(revisionWebTargetWithMultipleTags)
                .contentType(org.springframework.http.MediaType.APPLICATION_FORM_URLENCODED))
                .andExpect(status().isCreated());
    }

    @Test
    public void ShouldReturnBadRequestWhenAddingRevisionWithUnexpectedTag() throws Exception {
        mockMvc.perform(post(revisionWebTargetWithMultipleTags)
                .contentType(org.springframework.http.MediaType.APPLICATION_FORM_URLENCODED)
                .param(F_TAGS, Tags.ACCEPTANCE.getTag(), Tags.DELETED.getTag(), "undefined"))
                .andExpect(status().isBadRequest());
    }

    @Test
    public void shouldProperlyAddRevisionToDataSets() throws Exception {
        //given
        DataSet dataSet = dataSetService.createDataSet(dataProvider.getId(), "dataSetId", "DataSetDescription");
        dataSetService.addAssignment(dataProvider.getId(), dataSet.getId(), rep.getCloudId(), rep.getRepresentationName
                (), rep.getVersion());

        //when
        mockMvc.perform(post(revisionWebTarget)
                .contentType(MediaType.APPLICATION_JSON).content(toJson(revisionForDataProvider)))
                .andExpect(status().isCreated());
        //then
        verify(dataSetService, times(1)).updateAllRevisionDatasetsEntries(rep.getCloudId(), rep.getRepresentationName(), rep.getVersion(), revisionForDataProvider);
    }

    @Test
    public void shouldProperlyUpdateAllRevisionDatasetsEntries() throws Exception {
        //given
        DataSet dataSet = dataSetService.createDataSet(dataProvider.getId(), "dataSetId", "DataSetDescription");
        dataSetService.addAssignment(dataProvider.getId(), dataSet.getId(), rep.getCloudId(), rep.getRepresentationName
                (), rep.getVersion());

        //when
        mockMvc.perform(post(revisionWebTarget)
                .contentType(MediaType.APPLICATION_JSON).content(toJson(revisionForDataProvider)))
                .andExpect(status().isCreated());

        //then
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

        Revision revision = new Revision(TEST_REVISION_NAME, REVISION_PROVIDER_ID, date, true, false, false);
        Mockito.when(uisHandler.getProvider(providerId)).thenReturn(new DataProvider(providerId));
        Mockito.when(uisHandler.existsProvider(REVISION_PROVIDER_ID)).thenReturn(true);
        Mockito.when(uisHandler.existsCloudId(rep.getCloudId())).thenReturn(true);
        dataSetService.createDataSet(providerId, datasetId, "");
        dataSetService.addAssignment(providerId, datasetId, rep.getCloudId(), rep.getRepresentationName(), rep.getVersion());
        dataSetService.addDataSetsRevisions(providerId, datasetId, revision, rep.getRepresentationName(), rep.getCloudId());

        mockMvc.perform(delete(removeRevisionWebTarget)
                .queryParam(F_REVISION_TIMESTAMP, revisionTimeStamp)).andExpect(status().isNoContent());
    }

}