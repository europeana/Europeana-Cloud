package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.common.model.dps.ProcessedRecord;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.RecordExecutionSubmitService;
import eu.europeana.cloud.service.dps.storm.dao.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Date;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class RecordSubmitServiceTest {

  private static final String TOPOLOGY = "a_topology";
  private static final long TASK_ID = 1234567890;
  private static final String RECORD_ID = "recordId";
  private static final String TOPIC = "a_topology_1";

  private static final Date CURRENT_EXECUTION_START_TIME = new Date(0);

  private static final Date TIME_BEFORE_CURRENT_EXECUTION_START = new Date(-3000);

  @Mock
  private ProcessedRecordsDAO processedRecordsDAO;

  @Mock
  private RecordExecutionSubmitService kafkaSubmitService;

  private ProcessedRecord alreadyProcessedRecord = ProcessedRecord.builder().recordId(RECORD_ID).build();

  private DpsRecord record = DpsRecord.builder().taskId(TASK_ID).recordId(RECORD_ID).build();

  private SubmitTaskParameters parameters = SubmitTaskParameters.builder()
                                                                .taskInfo(TaskInfo.builder()
                                                                                  .topologyName(TOPOLOGY)
                                                                                  .startTimestamp(CURRENT_EXECUTION_START_TIME)
                                                                                  .build())
                                                                .topicName(TOPIC).build();

  @InjectMocks
  private RecordSubmitService service;

  @Test
  void shouldSubmitRecordThatNotAlreadyExists() {
    service.submitRecord(record, parameters);

    verify(kafkaSubmitService).submitRecord(record, TOPIC);
  }


  @Test
  void shouldNotSubmitAlreadyExistingRecord() {
    when(processedRecordsDAO.selectByPrimaryKey(anyLong(), anyString())).thenReturn(Optional.of(alreadyProcessedRecord));

    service.submitRecord(record, parameters);

    verify(kafkaSubmitService, never()).submitRecord(eq(record), eq(TOPIC));
  }

  @Test
  void shouldSaveRecordThatNotAlreadyExists() {
    service.submitRecord(record, parameters);

    verify(processedRecordsDAO).insert(anyLong(), anyString(), eq(0), anyString(),
            anyString(), eq(RecordState.QUEUED.toString()), anyString(), anyString());
  }

  @Test
  void shouldNotSaveAlreadyExistingRecord() {
    when(processedRecordsDAO.selectByPrimaryKey(anyLong(), anyString())).thenReturn(Optional.of(alreadyProcessedRecord));

    service.submitRecord(record, parameters);

    verify(processedRecordsDAO, never()).insert(anyLong(), anyString(), anyInt(), anyString(),
            anyString(), anyString(), anyString(), anyString());
  }


  @Test
  void shouldReturnTrueWhenSubmittingNewRecord() {
    boolean result = service.submitRecord(record, parameters);

    assertTrue(result);
  }

  @Test
  void shouldReturnFalseWhenSubmitDuplicatedRecord() {
    when(processedRecordsDAO.selectByPrimaryKey(anyLong(), anyString())).thenReturn(Optional.of(alreadyProcessedRecord));

    boolean result = service.submitRecord(record, parameters);

    assertFalse(result);
  }


  @Test
  void shouldReturnTrueWhenSubmitRetriedRecord() {
    parameters.setRestarted(true);
    alreadyProcessedRecord.setStarTime(TIME_BEFORE_CURRENT_EXECUTION_START);
    when(processedRecordsDAO.selectByPrimaryKey(anyLong(), anyString())).thenReturn(Optional.of(alreadyProcessedRecord));

    boolean result = service.submitRecord(record, parameters);

    assertTrue(result);
  }


  @Test
  void shouldReturnFalseWhenSubmitRetriedDuplicatedRecord() {
    parameters.setRestarted(true);
    alreadyProcessedRecord.setStarTime(CURRENT_EXECUTION_START_TIME);
    when(processedRecordsDAO.selectByPrimaryKey(anyLong(), anyString())).thenReturn(Optional.of(alreadyProcessedRecord));

    boolean result = service.submitRecord(record, parameters);

    assertFalse(result);
  }


}