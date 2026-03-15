package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.tuple.storm.*;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
class RevisionWriterBoltTest {

    private final int retryAttemptsCount = Optional.ofNullable(RetryableMethodExecutor.OVERRIDE_ATTEMPT_COUNT).orElse(8);

    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;

    @Mock(name = "revisionsClient")
    private RevisionServiceClient revisionServiceClient;

    @InjectMocks
    private RevisionWriterBolt revisionWriterBolt =
            new RevisionWriterBolt(new CassandraProperties(), "http://sample.ecloud.com/", "", "");

    @Captor
    private ArgumentCaptor<Revision> captor;


    @Test
    void nothingShouldBeAddedForEmptyRevisionsList() throws MCSException {
        Tuple anchorTuple = mock(TupleImpl.class);
        RevisionWriterBolt testMock = Mockito.spy(revisionWriterBolt);
        StormTaskTuple stormTaskTuple = new StormTaskTuple();
        stormTaskTuple.addParameter(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
        testMock.execute(anchorTuple, stormTaskTuple);

        Mockito.verify(revisionServiceClient, Mockito.times(0))
                .addRevision(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), any(Revision.class));
        Mockito.verify(outputCollector, Mockito.times(1))
                .emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), any(List.class));
    }

    @Test
    void methodForAddingRevisionsShouldBeExecuted() throws MCSException {
        Tuple anchorTuple = mock(TupleImpl.class);
        RevisionWriterBolt testMock = Mockito.spy(revisionWriterBolt);
        testMock.execute(anchorTuple, prepareTuple());
        Mockito.verify(revisionServiceClient, Mockito.times(1)).addRevision(any(), any(), any(), any(Revision.class));
        Mockito.verify(outputCollector, Mockito.times(1))
                .emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), any(List.class));
    }

    @Test
    void methodForAddingRevisionsShouldBeExecutedForDeletedRecord() throws MCSException {
        Tuple anchorTuple = mock(TupleImpl.class);
        RevisionWriterBolt testMock = Mockito.spy(revisionWriterBolt);
        StormTaskTuple stormTaskTuple = prepareTuple();
        stormTaskTuple.setMarkedAsDeleted(true);

        testMock.execute(anchorTuple, stormTaskTuple);

        Mockito.verify(revisionServiceClient, Mockito.times(1)).addRevision(any(), any(), any(), captor.capture());
        assertTrue(captor.getValue().isDeleted());
        Mockito.verify(outputCollector, Mockito.times(1))
                .emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), any(List.class));

    }

    @Test
    void malformedUrlExceptionShouldBeHandled() throws MCSException {
        Tuple anchorTuple = mock(TupleImpl.class);
        RevisionWriterBolt testMock = Mockito.spy(revisionWriterBolt);
        testMock.execute(anchorTuple, prepareTupleWithMalformedURL());
        Mockito.verify(revisionServiceClient, Mockito.times(0))
                .addRevision(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), any(Revision.class));
        Mockito.verify(outputCollector, Mockito.times(1))
                .emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), any(List.class));
    }

    @Test
    void mcsExceptionShouldBeHandledWithRetries() throws MCSException {
        Tuple anchorTuple = mock(TupleImpl.class);
        Mockito.when(revisionServiceClient.addRevision(any(), any(), any(), any(Revision.class))).thenThrow(MCSException.class);
        RevisionWriterBolt testMock = Mockito.spy(revisionWriterBolt);
        testMock.execute(anchorTuple, prepareTuple());
        Mockito.verify(revisionServiceClient, Mockito.times(retryAttemptsCount))
                .addRevision(any(), any(), any(), any(Revision.class));
        Mockito.verify(outputCollector, Mockito.times(1))
                .emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), any(List.class));

    }

    private StormTaskTuple prepareTuple() {
        ProcessingMetadata processingMetadata = new ProcessingMetadata();
        processingMetadata.setOutputRevision(new Revision());
        StormTaskTuple tuple = new StormTaskTuple(
                new TaskMetadata(123L, null, "sampleTaskName"),
                new FileMetadata("http://inputFileUrl", null),
                processingMetadata,
                new StormProcessingMetadata(),
                prepareTaskParameters(), null);
        tuple.addParameter(PluginParameterKeys.OUTPUT_URL, "http://sampleFileUrl");

        return tuple;
    }

    private StormTaskTuple prepareTupleWithMalformedURL() {
        ProcessingMetadata processingMetadata = new ProcessingMetadata();
        processingMetadata.setOutputRevision(new Revision());
        StormTaskTuple tuple = new StormTaskTuple(
                new TaskMetadata(123L, null, "sampleTaskName"),
                new FileMetadata("http://inputFileUrl", null),
                processingMetadata,
                new StormProcessingMetadata(),
                prepareTaskParameters(), null);
        tuple.addParameter(PluginParameterKeys.OUTPUT_URL, "malformedURL");
        return tuple;
    }

    Map<String, String> prepareTaskParameters() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
        return parameters;
    }
}
