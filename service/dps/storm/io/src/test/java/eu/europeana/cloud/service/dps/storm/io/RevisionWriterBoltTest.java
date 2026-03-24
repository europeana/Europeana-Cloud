package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import eu.europeana.cloud.service.dps.storm.tuple.common.RecordData;
import eu.europeana.cloud.service.dps.storm.tuple.common.processingData;
import eu.europeana.cloud.service.dps.storm.tuple.common.TaskData;
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
        CommonTaskTuple commonTaskTuple = new CommonTaskTuple();
        commonTaskTuple.addParameter(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
        testMock.execute(anchorTuple, commonTaskTuple);

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
        CommonTaskTuple commonTaskTuple = prepareTuple();
        commonTaskTuple.setMarkedAsDeleted(true);

        testMock.execute(anchorTuple, commonTaskTuple);

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

    private CommonTaskTuple prepareTuple() {
        CommonTaskTuple tuple = new CommonTaskTuple(
                new TaskData(123L, "sampleTaskName"),
                new RecordData("http://inputFileUrl", null),
                new processingData());
        tuple.setParameters(prepareTaskParameters());
        tuple.addParameter(PluginParameterKeys.OUTPUT_URL, "http://sampleFileUrl");
        tuple.setOutputRevision(new Revision());

        return tuple;
    }

    private CommonTaskTuple prepareTupleWithMalformedURL() {
        CommonTaskTuple tuple = new CommonTaskTuple(
                new TaskData(123L, "sampleTaskName"),
                new RecordData("http://inputFileUrl", null),
                new processingData());
        tuple.setParameters(prepareTaskParameters());
        tuple.addParameter(PluginParameterKeys.OUTPUT_URL, "malformedURL");
        tuple.setOutputRevision(new Revision());
        return tuple;
    }

    Map<String, String> prepareTaskParameters() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
        return parameters;
    }
}
