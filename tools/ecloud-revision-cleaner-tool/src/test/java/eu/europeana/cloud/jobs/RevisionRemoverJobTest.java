package eu.europeana.cloud.jobs;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.data.RevisionInformation;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.dps.test.TestHelper;
import org.apache.commons.lang3.time.FastDateFormat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.*;

import static eu.europeana.cloud.service.dps.test.TestConstants.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;


/**
 * Created by Tarek on 7/16/2019.
 */
@RunWith(MockitoJUnitRunner.class)
public class RevisionRemoverJobTest {

    private TestHelper testHelper = new TestHelper();
    @Mock
    private DataSetServiceClient dataSetServiceClient;
    @Mock
    private RecordServiceClient recordServiceClient;
    @Mock
    private RevisionServiceClient revisionServiceClient;
    @Mock
    private RevisionInformation revisionInformation;

    Date date = new Date();

    @InjectMocks
    RevisionRemoverJob revisionRemoverJob = new RevisionRemoverJob(dataSetServiceClient, recordServiceClient, revisionInformation, revisionServiceClient);

    @Test
    public void shouldRemoveRevisionsOnly() throws Exception {
        final int NUMBER_OF_REVISIONS = 3;
        final int NUMBER_OF_RESPONSES = 2;


        RevisionInformation revisionInformation = new RevisionInformation("DATASET", DATA_PROVIDER, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, getUTCDateString(date));
        revisionRemoverJob.setRevisionInformation(revisionInformation);

        Representation representation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);

        List<Representation> representations = new ArrayList<>(1);
        representations.add(representation);

        List<Revision> revisions = getRevisions(NUMBER_OF_REVISIONS);
        representation.setRevisions(revisions);


        ResultSlice<CloudTagsResponse> resultSlice = getCloudTagsResponseResultSlice(NUMBER_OF_RESPONSES);

        when(dataSetServiceClient.getDataSetRevisionsChunk(anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), any(), any())).thenReturn(resultSlice);
        when(recordServiceClient.getRepresentationsByRevision(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, getUTCDateString(date))).thenReturn(Arrays.asList(representation));
        when(recordServiceClient.getRepresentationsByRevision(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, getUTCDateString(date))).thenReturn(Arrays.asList(representation));
        Thread thread = new Thread(revisionRemoverJob);
        thread.start();
        thread.join();

        verify(revisionServiceClient, times(NUMBER_OF_RESPONSES * NUMBER_OF_REVISIONS)).deleteRevision(anyString(), anyString(), anyString(), anyString(), anyString(), anyString());
        verify(recordServiceClient, times(0)).deleteRepresentation(anyString(), anyString(), anyString());

    }

    private ResultSlice<CloudTagsResponse> getCloudTagsResponseResultSlice(int NUMBER_OF_RESPONSES) {
        List<CloudTagsResponse> cloudIdCloudTagsResponses = testHelper.prepareCloudTagsResponsesList();
        ResultSlice<CloudTagsResponse> resultSlice = new ResultSlice<>();
        resultSlice.setResults(cloudIdCloudTagsResponses);
        resultSlice.setNextSlice(null);
        assertEquals(cloudIdCloudTagsResponses.size(), NUMBER_OF_RESPONSES);
        return resultSlice;
    }


    @Test
    public void shouldRemoveEntireRepresentation() throws Exception {
        final int NUMBER_OF_REVISIONS = 1;
        final int NUMBER_OF_RESPONSES = 2;


        RevisionInformation revisionInformation = new RevisionInformation("DATASET", DATA_PROVIDER, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, getUTCDateString(date));
        revisionRemoverJob.setRevisionInformation(revisionInformation);

        Representation representation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, date);

        List<Representation> representations = new ArrayList<>(1);
        representations.add(representation);

        List<Revision> revisions = getRevisions(NUMBER_OF_REVISIONS);
        representation.setRevisions(revisions);


        ResultSlice<CloudTagsResponse> resultSlice = getCloudTagsResponseResultSlice(NUMBER_OF_RESPONSES);

        when(dataSetServiceClient.getDataSetRevisionsChunk(anyString(), anyString(), anyString(), anyString(), anyString(), anyString(), any(), any())).thenReturn(resultSlice);
        when(recordServiceClient.getRepresentationsByRevision(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, getUTCDateString(date))).thenReturn(Arrays.asList(representation));
        when(recordServiceClient.getRepresentationsByRevision(SOURCE + CLOUD_ID2, SOURCE + REPRESENTATION_NAME, REVISION_NAME, REVISION_PROVIDER, getUTCDateString(date))).thenReturn(Arrays.asList(representation));
        Thread thread = new Thread(revisionRemoverJob);
        thread.start();
        thread.join();

        verify(revisionServiceClient, times(0)).deleteRevision(anyString(), anyString(), anyString(), anyString(), anyString(), anyString());
        verify(recordServiceClient, times(NUMBER_OF_RESPONSES)).deleteRepresentation(anyString(), anyString(), anyString());

    }

    private List<Revision> getRevisions(int revisionCount) {
        List<Revision> revisions = new ArrayList<>(revisionCount);
        for (int i = 0; i < revisionCount; i++) {
            Revision revision1 = new Revision(REVISION_NAME, REVISION_PROVIDER);
            revision1.setCreationTimeStamp(date);
            revisions.add(revision1);
        }
        return revisions;
    }


    public static String getUTCDateString(Date date) {
        final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";
        FastDateFormat formatter = FastDateFormat.getInstance(DATE_FORMAT, TimeZone.getTimeZone("UTC"));
        return formatter.format(date);
    }

}