package eu.europeana.cloud.service.dps.storm.io;

import com.google.gson.Gson;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;

/**
 * Created by Tarek on 9/24/2019.
 */
public class IndexingRevisionWriterTest {
    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;

    @Mock(name = "revisionsClient")
    private RevisionServiceClient revisionServiceClient;

    @InjectMocks
    private IndexingRevisionWriter indexingRevisionWriter = new IndexingRevisionWriter("http://sample.ecloud.com/", "sampleMessage");

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

    final ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);


    @Test
    public void nothingShouldBeAddedForEmptyRevisionsList() throws MCSException, URISyntaxException, MalformedURLException {
        Tuple anchorTuple = mock(TupleImpl.class);
        RevisionWriterBolt testMock = Mockito.spy(indexingRevisionWriter);
        testMock.execute(anchorTuple, prepareTupleWithEmptyRevisions());
        Mockito.verify(revisionServiceClient, Mockito.times(0)).addRevision(anyString(), anyString(), anyString(), Mockito.any(Revision.class), anyString(), anyString());
        Mockito.verify(outputCollector, Mockito.times(0)).emit(Mockito.any(List.class));
        Mockito.verify(outputCollector, Mockito.times(1)).emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), Mockito.any(List.class));
        Mockito.verify(outputCollector).emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), captor.capture());
        List list = captor.getValue();
        assertNotNull(list);
        assertEquals(3, list.size());
        Map<String, String> parameters = (Map<String, String>) list.get(2);
        assertEquals("SUCCESS", parameters.get(NotificationParameterKeys.STATE));
        assertNotNull(parameters.get(NotificationParameterKeys.DATA_SET_CLEANING_PARAMETERS));
        assertNotNull(parameters.get(NotificationParameterKeys.DPS_URL));
        assertNotNull(parameters.get(NotificationParameterKeys.AUTHORIZATION_HEADER));

    }

    @Test
    public void methodForAddingRevisionsShouldBeExecuted() throws MalformedURLException, MCSException {
        Tuple anchorTuple = mock(TupleImpl.class);
        RevisionWriterBolt testMock = Mockito.spy(indexingRevisionWriter);
        testMock.execute(anchorTuple, prepareTuple());
        Mockito.verify(revisionServiceClient, Mockito.times(1)).addRevision(any(), any(), any(), Mockito.any(Revision.class), any(), any());
        Mockito.verify(outputCollector, Mockito.times(0)).emit(Mockito.any(List.class));
        Mockito.verify(outputCollector, Mockito.times(1)).emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), Mockito.any(List.class));
        Mockito.verify(outputCollector).emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), captor.capture());
        List list = captor.getValue();
        assertNotNull(list);
        assertEquals(3, list.size());
        Map<String, String> parameters = (Map<String, String>) list.get(2);
        assertEquals("SUCCESS", parameters.get(NotificationParameterKeys.STATE));
        assertNotNull(parameters.get(NotificationParameterKeys.DATA_SET_CLEANING_PARAMETERS));
        assertNotNull(parameters.get(NotificationParameterKeys.DPS_URL));
        assertNotNull(parameters.get(NotificationParameterKeys.AUTHORIZATION_HEADER));
    }

    @Test
    public void malformedUrlExceptionShouldBeHandled() throws MalformedURLException, MCSException {
        Tuple anchorTuple = mock(TupleImpl.class);
        RevisionWriterBolt testMock = Mockito.spy(indexingRevisionWriter);
        testMock.execute(anchorTuple, prepareTupleWithMalformedURL());
        Mockito.verify(revisionServiceClient, Mockito.times(0)).addRevision(anyString(), anyString(), anyString(), Mockito.any(Revision.class), anyString(), anyString());
        Mockito.verify(outputCollector, Mockito.times(1)).emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), Mockito.any(List.class));
        Mockito.verify(outputCollector).emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), captor.capture());
        List list = captor.getValue();
        assertNotNull(list);
        assertEquals(3, list.size());
        Map<String, String> parameters = (Map<String, String>) list.get(2);
        assertEquals("ERROR", parameters.get("state"));


    }

    @Test
    public void mcsExceptionShouldBeHandledWithRetries() throws MalformedURLException, MCSException {
        Tuple anchorTuple = mock(TupleImpl.class);
        Mockito.when(revisionServiceClient.addRevision(any(), any(), any(), Mockito.any(Revision.class), any(), any())).thenThrow(MCSException.class);
        RevisionWriterBolt testMock = Mockito.spy(indexingRevisionWriter);
        testMock.execute(anchorTuple, prepareTuple());
        Mockito.verify(revisionServiceClient, Mockito.times(4)).addRevision(any(), any(), any(), Mockito.any(Revision.class), any(), any());
        Mockito.verify(outputCollector, Mockito.times(1)).emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME),any(Tuple.class), Mockito.any(List.class));
    }

    private StormTaskTuple prepareTuple() {

        StormTaskTuple tuple = new StormTaskTuple(123L, "sampleTaskName", "http://inputFileUrl", null, prepareTaskParameters(), new Revision());
        return tuple;
    }

    private StormTaskTuple prepareTupleWithMalformedURL() {
        StormTaskTuple tuple = new StormTaskTuple(123L, "sampleTaskName", "malformed", null, prepareTaskParameters(), new Revision());
        return tuple;
    }

    private StormTaskTuple prepareTupleWithEmptyRevisions() {
        StormTaskTuple tuple = new StormTaskTuple(123L, "sampleTaskName", "http://inputFileUrl", null, prepareTaskParameters(), null);
        return tuple;
    }


    private DataSetCleanerParameters prepareDataSetCleanerParameters() {
        DataSetCleanerParameters dataSetCleanerParameters = new DataSetCleanerParameters();
        dataSetCleanerParameters.setCleaningDate(new Date());
        dataSetCleanerParameters.setDataSetId("DATASET_ID");
        dataSetCleanerParameters.setUsingAltEnv(true);
        dataSetCleanerParameters.setTargetIndexingEnv("PREVIEW");
        return dataSetCleanerParameters;
    }

    Map<String, String> prepareTaskParameters() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.DATA_SET_CLEANING_PARAMETERS, new Gson().toJson(prepareDataSetCleanerParameters()));
        parameters.put(PluginParameterKeys.DPS_URL, "DPS_URL");
        parameters.put(PluginParameterKeys.AUTHORIZATION_HEADER, "AUTHORIZATION_HEADER");
        parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
        return parameters;
    }

}


