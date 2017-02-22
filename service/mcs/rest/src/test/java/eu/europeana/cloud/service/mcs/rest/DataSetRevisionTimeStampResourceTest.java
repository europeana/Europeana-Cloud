package eu.europeana.cloud.service.mcs.rest;

import com.google.common.collect.ImmutableMap;
import eu.europeana.cloud.common.model.*;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.ApplicationContextUtils;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.service.mcs.rest.exceptionmappers.RevisionIsNotValidExceptionMapper;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
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
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static eu.europeana.cloud.common.web.ParamConstants.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;


/**
 * RevisionResourceTest
 */
@RunWith(CassandraTestRunner.class)
public class DataSetRevisionTimeStampResourceTest extends JerseyTest {

    private RecordService recordService;
    private Representation rep;

    private WebTarget revisionAndRepresenttaionWebTargetInsideDataSet;

    private final static String TEST_REVISION_NAME = "revisionNameTest";
    private final static String DATE_SET_NAME = "dataSetId";
    private final static String REVISION_PROVIDER_ID = "revisionProviderId";
    private UISClientHandler uisHandler;
    private DataSetService dataSetService;
    private DataProvider dataProvider;
    private Revision revisionForDataProvider;
    private String revisionAndRepresentationPath;


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
        revisionForDataProvider = new Revision(TEST_REVISION_NAME, REVISION_PROVIDER_ID, new Date(), true, true, true);

        String dataSetPath = DataSetResource.class.getAnnotation(Path.class).value();

        revisionAndRepresentationPath = dataSetPath + "/revision" + "/{" + P_REVISION_NAME + "}/" + REVISION_PROVIDER + "/{" + P_REVISION_PROVIDER_ID + "}/" + REPRESENTATIONS + "/{" + P_REPRESENTATIONNAME + "}";

        Map<String, Object> revisionAndRepresentation = ImmutableMap
                .<String, Object>of(P_PROVIDER,
                        dataProvider.getId(), P_DATASET,
                        DATE_SET_NAME, P_REPRESENTATIONNAME,
                        rep.getRepresentationName(), P_REVISION_NAME,
                        TEST_REVISION_NAME, P_REVISION_PROVIDER_ID,
                        REVISION_PROVIDER_ID);


        revisionAndRepresenttaionWebTargetInsideDataSet = target(revisionAndRepresentationPath).resolveTemplates(revisionAndRepresentation);


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
        return new JerseyConfig().property("contextConfigLocation",
                "classpath:spiedPersistentServicesTestContext.xml")
                .registerClasses(RevisionIsNotValidExceptionMapper.class);
    }

    @Override
    protected void configureClient(ClientConfig config) {
        config.register(MultiPartFeature.class);
    }


    @Test
    public void shouldProperlyReturnCloudIdAndTimestamp() throws Exception {
        List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = getDataSetCloudIdsByRepresentationAndRevisionSuccessfully(null);
        assertEquals(cloudIdAndTimestampResponseList.get(0).getCloudId(), rep.getCloudId());
    }

    @Test
    public void shouldProperlyReturnCloudIdAndTimestampWhenDeleted() throws Exception {
        List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = getDataSetCloudIdsByRepresentationAndRevisionSuccessfully(true);
        assertEquals(cloudIdAndTimestampResponseList.get(0).getCloudId(), rep.getCloudId());
    }

    @Test
    public void shouldReturnEmptyResults() throws Exception {
        List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = getDataSetCloudIdsByRepresentationAndRevisionSuccessfully(false);
        assertTrue(cloudIdAndTimestampResponseList.isEmpty());
    }

    private List<CloudIdAndTimestampResponse> getDataSetCloudIdsByRepresentationAndRevisionSuccessfully(Boolean isDeleted) throws ProviderNotExistsException, DataSetAlreadyExistsException, RevisionIsNotValidException, DataSetNotExistsException, RepresentationNotExistsException {
        //given
        PrepareTest(dataProvider.getId(), DATE_SET_NAME);

        makeExistsProviderPass();

        //when
        Response response = revisionAndRepresenttaionWebTargetInsideDataSet.queryParam(F_START_FROM, null).queryParam(IS_DELETED, isDeleted).request().get();
        //then
        assertSuccessfulCallToLatestRevisionAndRepresentation(response);
        ResultSlice<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseResultSlice = response.readEntity(ResultSlice.class);
        return cloudIdAndTimestampResponseResultSlice.getResults();

    }


    private void assertSuccessfulCallToLatestRevisionAndRepresentation(Response response) {
        assertNotNull(response);
        assertEquals(response.getStatus(), 200);
    }

    private void makeExistsProviderPass() {
        Mockito.doReturn(true).when(uisHandler)
                .existsProvider("1");
    }

    private void PrepareTest(String providerId, String datasetName) throws ProviderNotExistsException, DataSetAlreadyExistsException, RevisionIsNotValidException, DataSetNotExistsException, RepresentationNotExistsException {
        DataSet dataSet = dataSetService.createDataSet(providerId, datasetName, "DataSetDescription");
        recordService.addRevision(rep.getCloudId(), rep.getRepresentationName(), rep.getVersion(), revisionForDataProvider);

        dataSetService.addAssignment(providerId, dataSet.getId(), rep.getCloudId(), rep.getRepresentationName
                (), rep.getVersion());
    }

    @Test
    public void shouldReturnDataSetNotExistError() throws Exception {

        PrepareTest(dataProvider.getId(), "None_Existed_Dataset");
        makeExistsProviderPass();

        Response response = revisionAndRepresenttaionWebTargetInsideDataSet.queryParam(F_START_FROM, null).queryParam(IS_DELETED, null).request().get();
        assertNotNull(response);
        assertEquals(response.getStatus(), 404);
        ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
        assertEquals(McsErrorCode.DATASET_NOT_EXISTS.toString(), errorInfo.getErrorCode());

    }

    @Test
    public void shouldReturnProviderNotExistError() throws Exception {
        //given
        PrepareTest("1", DATE_SET_NAME);

        //when

        Map<String, Object> revisionAndRepresentationNoneExistedProviderId = ImmutableMap
                .<String, Object>of(P_PROVIDER,
                        dataProvider.getId(), P_DATASET,
                        DATE_SET_NAME, P_REPRESENTATIONNAME,
                        rep.getRepresentationName(), P_REVISION_NAME,
                        TEST_REVISION_NAME, P_REVISION_PROVIDER_ID,
                        dataProvider.getId());

        revisionAndRepresenttaionWebTargetInsideDataSet = target(revisionAndRepresentationPath).resolveTemplates(revisionAndRepresentationNoneExistedProviderId);
        Response response = revisionAndRepresenttaionWebTargetInsideDataSet.queryParam(F_START_FROM, null).queryParam(IS_DELETED, null).request().get();
        //then
        assertNotNull(response);
        assertEquals(response.getStatus(), 404);
        ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
        assertEquals(McsErrorCode.PROVIDER_NOT_EXISTS.toString(), errorInfo.getErrorCode());


    }
}