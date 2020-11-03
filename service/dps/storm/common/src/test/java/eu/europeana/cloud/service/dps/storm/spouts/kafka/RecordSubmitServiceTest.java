package eu.europeana.cloud.service.dps.storm.spouts.kafka;

import eu.europeana.cloud.common.model.dps.ProcessedRecord;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.RecordExecutionSubmitService;
import eu.europeana.cloud.service.dps.storm.utils.ProcessedRecordsDAO;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Date;
import java.util.Optional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RecordSubmitServiceTest {

    private static final String TOPOLOGY = "a_topology";
    private static final long TASK_ID = 1234567890;
    private static final String RECORD_ID = "recordId";
    private static final String TOPIC = "a_topology_1";

    private static final Date SENT_DATE = new Date(0);

    private static final Date NOT_SO_OLD_DATE = new Date(-2000);

    private static final Date OLD_DATE = new Date(-3000);

    @Mock
    private ProcessedRecordsDAO processedRecordsDAO;

    @Mock
    private RecordExecutionSubmitService kafkaSubmitService;

    private ProcessedRecord alreadyProcessedRecord = ProcessedRecord.builder().recordId(RECORD_ID).build();

    private DpsRecord record = DpsRecord.builder().taskId(TASK_ID).recordId(RECORD_ID).build();

    private SubmitTaskParameters parameters = SubmitTaskParameters.builder().topologyName(TOPOLOGY).topicName(TOPIC)
            .sentTime(SENT_DATE).build();

    @InjectMocks
    private RecordSubmitService service;

    @Test
    public void verifySubmitRecordThatNotAlreadyExists() {
        service.submitRecord(record, parameters);

        verify(kafkaSubmitService).submitRecord(eq(record), eq(TOPIC));
    }


    @Test
    public void verifyNotSubmitAlreadyExistingRecord() {
        when(processedRecordsDAO.selectByPrimaryKey(anyLong(), anyString())).thenReturn(Optional.of(alreadyProcessedRecord));

        service.submitRecord(record, parameters);

        verify(kafkaSubmitService, never()).submitRecord(eq(record), eq(TOPIC));
    }

    @Test
    public void verifySaveRecordThatNotAlreadyExists() {
        service.submitRecord(record, parameters);

        verify(processedRecordsDAO).insert(anyLong(), anyString(), eq(0), anyString(),
                anyString(), eq(RecordState.QUEUED.toString()), anyString(), anyString());
    }

    @Test
    public void verifyNotSaveAlreadyExistingRecord() {
        when(processedRecordsDAO.selectByPrimaryKey(anyLong(), anyString())).thenReturn(Optional.of(alreadyProcessedRecord));

        service.submitRecord(record, parameters);

        verify(processedRecordsDAO, never()).insert(anyLong(), anyString(), anyInt(), anyString(),
                anyString(), anyString(), anyString(), anyString());
    }


    @Test
    public void verifyReturnTrueWhenSubmitingNewRecord() {
        boolean result = service.submitRecord(record, parameters);

        assertTrue(result);
    }

    @Test
    public void verifyReturnFalseWhenSubmitDuplicatedRecord() {
        when(processedRecordsDAO.selectByPrimaryKey(anyLong(), anyString())).thenReturn(Optional.of(alreadyProcessedRecord));

        boolean result = service.submitRecord(record, parameters);

        assertFalse(result);
    }


    @Test
    public void verifyReturnTrueWhenSubmitRetriedRecord() {
        parameters.setRestarted(true);
        alreadyProcessedRecord.setStarTime(OLD_DATE);
        when(processedRecordsDAO.selectByPrimaryKey(anyLong(), anyString())).thenReturn(Optional.of(alreadyProcessedRecord));

        boolean result = service.submitRecord(record, parameters);

        assertTrue(result);
    }


    @Test
    public void verifyReturnFalseWhenSubmitRetriedDuplicatedRecord() {
        parameters.setRestarted(true);
        alreadyProcessedRecord.setStarTime(NOT_SO_OLD_DATE);
        when(processedRecordsDAO.selectByPrimaryKey(anyLong(), anyString())).thenReturn(Optional.of(alreadyProcessedRecord));

        boolean result = service.submitRecord(record, parameters);

        assertFalse(result);
    }


}