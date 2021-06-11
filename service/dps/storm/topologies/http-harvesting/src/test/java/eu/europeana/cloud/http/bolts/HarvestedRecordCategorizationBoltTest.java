package eu.europeana.cloud.http.bolts;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.incremental.CategorizationParameters;
import eu.europeana.cloud.service.dps.storm.incremental.CategorizationResult;
import org.apache.commons.io.FileUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.io.File;
import java.io.IOException;
import java.time.Instant;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class HarvestedRecordCategorizationBoltTest {

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);
    @Mock(name = "harvestedRecordCategorizationService")
    private HarvestedRecordCategorizationService harvestedRecordCategorizationService;
    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;

    @InjectMocks
    private HarvestedRecordCategorizationBolt harvestedRecordCategorizationBolt = new HarvestedRecordCategorizationBolt(null);

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this); // initialize all the @Mock objects
    }

    @Test
    public void shouldForwardTupleToNextBoltInCaseOfNonIncrementalProcessing() throws IOException {
        //given
        Tuple anchorTuple = mock(TupleImpl.class);
        StormTaskTuple tuple = prepareNonIncrementalTuple();
        when(harvestedRecordCategorizationService.categorize(any())).thenReturn(
                CategorizationResult
                        .builder()
                        .category(CategorizationResult.Category.ELIGIBLE_FOR_PROCESSING)
                        .build());
        //when
        harvestedRecordCategorizationBolt.execute(anchorTuple, tuple);
        //then
        verify(outputCollector, never()).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), anyList());
        verify(outputCollector, times(1)).emit(any(Tuple.class), captor.capture());
    }

    @Test
    public void shouldForwardTupleToNextBoltInCaseOfNonExistingIncrementalParameter() throws IOException {
        //given
        Tuple anchorTuple = mock(TupleImpl.class);
        StormTaskTuple tuple = prepareTupleWithoutIncrementalParameter();
        when(harvestedRecordCategorizationService.categorize(any())).thenReturn(
                CategorizationResult
                        .builder()
                        .category(CategorizationResult.Category.ELIGIBLE_FOR_PROCESSING)
                        .build());
        //when
        harvestedRecordCategorizationBolt.execute(anchorTuple, tuple);
        //then
        verify(outputCollector, never()).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), anyList());
        verify(outputCollector, times(1)).emit(any(Tuple.class), anyList());
    }

    @Test
    public void shouldCategorizeMessageAsEligibleForProcessing() throws IOException {
        //given
        Tuple anchorTuple = mock(TupleImpl.class);
        StormTaskTuple tuple = prepareTupleWithoutIncrementalParameter();
        when(harvestedRecordCategorizationService.categorize(any())).thenReturn(
                CategorizationResult
                        .builder()
                        .category(CategorizationResult.Category.ELIGIBLE_FOR_PROCESSING)
                        .build());
        //when
        harvestedRecordCategorizationBolt.execute(anchorTuple, tuple);
        //then
        verify(outputCollector, never()).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), anyList());
        verify(outputCollector, times(1)).emit(any(Tuple.class), anyList());
    }

    @Test
    public void shouldCategorizeMessageAsAlreadyProcessed() throws IOException {
        //given
        Tuple anchorTuple = mock(TupleImpl.class);
        StormTaskTuple tuple = prepareTupleWithIncrementalParameter();
        when(harvestedRecordCategorizationService.categorize(any())).thenReturn(
                CategorizationResult
                        .builder()
                        .category(CategorizationResult.Category.ALREADY_PROCESSED)
                        .categorizationParameters(
                                CategorizationParameters
                                        .builder()
                                        .recordDateStamp(Instant.now())
                                        .build())
                        .build());
        //when
        harvestedRecordCategorizationBolt.execute(anchorTuple, tuple);
        //then
        verify(outputCollector, times(1)).emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), anyList());
        verify(outputCollector, never()).emit(any(Tuple.class), anyList());
    }


    private StormTaskTuple prepareNonIncrementalTuple() throws IOException {
        StormTaskTuple tuple = new StormTaskTuple();
        tuple.setTaskId(1);
        tuple.setFileData(FileUtils.readFileToByteArray(new File(ClassLoader.getSystemResource("Lithuania_1.xml").getFile())));
        tuple.addParameter(PluginParameterKeys.INCREMENTAL_HARVEST, "false");
        tuple.addParameter(PluginParameterKeys.RECORD_DATESTAMP, Instant.now().toString());
        tuple.addParameter(PluginParameterKeys.HARVEST_DATE, Instant.now().toString());
        return tuple;
    }

    private StormTaskTuple prepareTupleWithoutIncrementalParameter() throws IOException {
        StormTaskTuple tuple = new StormTaskTuple();
        tuple.setFileData(FileUtils.readFileToByteArray(new File(ClassLoader.getSystemResource("Lithuania_1.xml").getFile())));
        tuple.addParameter(PluginParameterKeys.RECORD_DATESTAMP, Instant.now().toString());
        tuple.addParameter(PluginParameterKeys.HARVEST_DATE, Instant.now().toString());
        return tuple;
    }

    private StormTaskTuple prepareTupleWithIncrementalParameter() throws IOException {
        StormTaskTuple tuple = new StormTaskTuple();
        tuple.setFileData(FileUtils.readFileToByteArray(new File(ClassLoader.getSystemResource("Lithuania_1.xml").getFile())));
        tuple.addParameter(PluginParameterKeys.INCREMENTAL_HARVEST, "true");
        tuple.addParameter(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "10");
        tuple.addParameter(PluginParameterKeys.RECORD_DATESTAMP, Instant.now().toString());
        tuple.addParameter(PluginParameterKeys.HARVEST_DATE, Instant.now().toString());

        return tuple;
    }
}