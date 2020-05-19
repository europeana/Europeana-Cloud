package eu.europeana.cloud.service.mcs.rest;

import com.google.common.collect.ImmutableMap;
import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RevisionIsNotValidException;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
import eu.europeana.cloud.test.CassandraTestRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.test.web.servlet.ResultActions;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static eu.europeana.cloud.common.web.ParamConstants.DATA_SET_ID;
import static eu.europeana.cloud.common.web.ParamConstants.IS_DELETED;
import static eu.europeana.cloud.common.web.ParamConstants.PROVIDER_ID;
import static eu.europeana.cloud.common.web.ParamConstants.REPRESENTATION_NAME;
import static eu.europeana.cloud.common.web.ParamConstants.REVISION_NAME;
import static eu.europeana.cloud.common.web.ParamConstants.REVISION_PROVIDER_ID;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.DATA_SET_BY_REPRESENTATION_REVISION;
import static eu.europeana.cloud.service.mcs.utils.MockMvcUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.reset;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


/**
 * RevisionResourceTest
 */
@RunWith(CassandraTestRunner.class)
public class DataSetRevisionTimeStampResourceTest extends CassandraBasedAbstractResourceTest {

    private RecordService recordService;
    private Representation rep;

    //private String revisionAndRepresenttaionWebTargetInsideDataSet;

    private final static String TEST_REVISION_NAME = "revisionNameTest";
    private final static String DATE_SET_NAME = "dataSetId";
    private final static String TEST_REVISION_PROVIDER_ID = "revisionProviderId";
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
        revisionForDataProvider = new Revision(TEST_REVISION_NAME, REVISION_PROVIDER_ID, new Date(), true, true, true);
    }

    @After
    public void cleanUp() throws Exception {
        recordService.deleteRepresentation(rep.getCloudId(),
                rep.getRepresentationName());
        reset(recordService);
        reset(dataSetService);
    }

    //new JerseyConfig().property("contextConfigLocation", "classpath:spiedPersistentServicesTestContext.xml").registerClasses(RevisionIsNotValidExceptionMapper.class);
//    @Override
//    protected void configureClient(ClientConfig config) {
//        config.register(MultiPartFeature.class);
//    }


    @Test
    public void shouldProperlyReturnCloudIdAndTimestamp() throws Exception {
        List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = getDataSetCloudIdsByRepresentationAndRevisionSuccessfully(true);
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

    private List<CloudIdAndTimestampResponse> getDataSetCloudIdsByRepresentationAndRevisionSuccessfully(Boolean isDeleted) throws Exception {
        //given
        PrepareTest(dataProvider.getId(), DATE_SET_NAME);

        makeExistsProviderPass();

        //when
        ResultActions response = mockMvc.perform(get(DATA_SET_BY_REPRESENTATION_REVISION, dataProvider.getId(),
                DATE_SET_NAME, rep.getRepresentationName(), TEST_REVISION_NAME, TEST_REVISION_PROVIDER_ID)
                .queryParam(IS_DELETED, isDeleted.toString()))
                .andExpect(status().isOk());
        //then
        return responseContentAsCloudIdAndTimestampResultSlice(response).getResults();

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

        ResultActions response = mockMvc.perform(get(DATA_SET_BY_REPRESENTATION_REVISION, dataProvider.getId(),
                DATE_SET_NAME, rep.getRepresentationName(), TEST_REVISION_NAME, TEST_REVISION_PROVIDER_ID))
                .andExpect(status().isNotFound());


        ErrorInfo errorInfo = responseContentAsErrorInfo(response);
        assertEquals(McsErrorCode.DATASET_NOT_EXISTS.toString(), errorInfo.getErrorCode());
    }

    @Test
    public void shouldReturnProviderNotExistError() throws Exception {
        //given
        PrepareTest("1", DATE_SET_NAME);

        //when

        Map<String, Object> revisionAndRepresentationNoneExistedProviderId = ImmutableMap
                .<String, Object>of(PROVIDER_ID,
                        dataProvider.getId(), DATA_SET_ID,
                        DATE_SET_NAME, REPRESENTATION_NAME,
                        rep.getRepresentationName(), REVISION_NAME,
                        TEST_REVISION_NAME, REVISION_PROVIDER_ID,
                        dataProvider.getId());

        ResultActions response = mockMvc.perform(get(DATA_SET_BY_REPRESENTATION_REVISION, dataProvider.getId(),
                DATE_SET_NAME, rep.getRepresentationName(), TEST_REVISION_NAME, dataProvider.getId()))
                .andExpect(status().isNotFound());
        //then
        ErrorInfo errorInfo = responseContentAsErrorInfo(response);
        assertEquals(McsErrorCode.PROVIDER_NOT_EXISTS.toString(), errorInfo.getErrorCode());


    }
}