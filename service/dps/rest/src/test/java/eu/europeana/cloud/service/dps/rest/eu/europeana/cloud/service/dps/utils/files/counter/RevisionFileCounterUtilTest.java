package eu.europeana.cloud.service.dps.rest.eu.europeana.cloud.service.dps.utils.files.counter;

import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.dps.ApplicationContextUtils;
import eu.europeana.cloud.service.dps.storm.utils.DateHelper;
import eu.europeana.cloud.service.dps.test.TestHelper;
import eu.europeana.cloud.service.dps.utils.files.counter.RevisionFileCounterUtil;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.net.URISyntaxException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.*;
import static eu.europeana.cloud.service.dps.test.TestConstants.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {
        "classpath:spiedDpsTestContext.xml"
})
public class RevisionFileCounterUtilTest {
    private RecordServiceClient recordServiceClient;
    private DataSetServiceClient dataSetServiceClient;
    private RevisionFileCounterUtil revisionFileCounterUtil;
    private UrlParser urlParser;
    private static Date date = new Date();
    private TestHelper testHelper;

    @Before
    public void init() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        recordServiceClient = applicationContext.getBean(RecordServiceClient.class);
        dataSetServiceClient = applicationContext.getBean(DataSetServiceClient.class);
        revisionFileCounterUtil = applicationContext.getBean(RevisionFileCounterUtil.class);
        urlParser = applicationContext.getBean(UrlParser.class);
        testHelper = new TestHelper();
    }

    @Test
    public void getFilesCountForSpecificRevisionsTest() throws URISyntaxException, MCSException {
        prepareMocksForSpecidicRevisions();
        int fileCount = revisionFileCounterUtil.getFilesCountForSpecificRevisions(SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, urlParser, DateHelper.getUTCDateString(date));
        assertEquals(fileCount, 2);
    }

    @Test
    public void getFilesCountForTheLatestRevisionsTest() throws URISyntaxException, MCSException {
        prepareMocksForLatestRevisions();
        int fileCount = revisionFileCounterUtil.getFilesCountForTheLatestRevisions(SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, urlParser);
        assertEquals(fileCount, 2);
    }


    private void prepareMocksForSpecidicRevisions() throws URISyntaxException, MCSException {

        Representation firstRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        Representation secondRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);

        List<CloudTagsResponse> cloudIdCloudTagsResponses = testHelper.prepareCloudTagsResponsesList();
        when(dataSetServiceClient.getDataSetRevisions(anyString(), anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(cloudIdCloudTagsResponses);

        RepresentationRevisionResponse firstRepresentationRevisionResponse = new RepresentationRevisionResponse(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, REVISION_PROVIDER, REVISION_NAME, date);
        RepresentationRevisionResponse secondRepresentationRevisionResponse = new RepresentationRevisionResponse(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, REVISION_PROVIDER, REVISION_NAME, date);


        when(recordServiceClient.getRepresentationRevision(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(firstRepresentationRevisionResponse);
        when(recordServiceClient.getRepresentationRevision(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(secondRepresentationRevisionResponse);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(firstRepresentation);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(secondRepresentation);

    }

    private void prepareMocksForLatestRevisions() throws URISyntaxException, MCSException {

        Representation firstRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);
        Representation secondRepresentation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);

        List<CloudIdAndTimestampResponse> cloudIdAndTimestampResponseList = testHelper.prepareCloudIdAndTimestampResponseList(date);

        RepresentationRevisionResponse firstRepresentationRevisionResponse = new RepresentationRevisionResponse(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, REVISION_PROVIDER, REVISION_NAME, date);
        RepresentationRevisionResponse secondRepresentationRevisionResponse = new RepresentationRevisionResponse(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, REVISION_PROVIDER, REVISION_NAME, date);


        when(dataSetServiceClient.getLatestDataSetCloudIdByRepresentationAndRevision(anyString(), anyString(), anyString(), anyString(), anyString(), anyBoolean())).thenReturn(cloudIdAndTimestampResponseList);
        when(recordServiceClient.getRepresentationRevision(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(firstRepresentationRevisionResponse);
        when(recordServiceClient.getRepresentationRevision(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, DateHelper.getUTCDateString(date))).thenReturn(secondRepresentationRevisionResponse);

        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(firstRepresentation);
        when(recordServiceClient.getRepresentation(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION)).thenReturn(secondRepresentation);

    }

}