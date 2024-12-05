package eu.europeana.cloud.service.dps.storm.topologies.depublication;

import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.NOTIFICATION_STREAM_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexedRecordRemover;
import eu.europeana.cloud.service.dps.storm.NotificationParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.utils.DepublicationReason;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DepublicationBoltTest {

  public static final long TASK_ID = 10L;
  private static final String METIS_DATASET_ID = "100";
  private static final String RECORD_ID = "/100/record1";
  private static final Date LATEST_HARVEST_DATE = new Date(2000);
  private static final UUID LATEST_HARVEST_MD5 = UUID.randomUUID();
  private static final DepublicationReason REASON = DepublicationReason.PERMISSION_ISSUES;
  private static final Map<String, String> INPUT_TUPLE_PARAMETERS = Map.of(
      PluginParameterKeys.METIS_DATASET_ID, METIS_DATASET_ID,
      PluginParameterKeys.DEPUBLICATION_REASON, REASON.name(),
      PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "0");

  public static final StormTaskTuple INPUT_TUPLE = new StormTaskTuple(TASK_ID, "taskName",
      RECORD_ID, null, INPUT_TUPLE_PARAMETERS, null);

  @Mock(name = "outputCollector")
  private OutputCollector outputCollector;

  @Mock
  private IndexedRecordRemover indexedRecordRemover;

  @Mock
  private Properties indexingProperties;
  @InjectMocks
  private final DepublicationBolt depublicationBolt = new DepublicationBolt(null, indexingProperties);
  @Mock
  private HarvestedRecordsDAO harvestedRecordsDAO;
  @Mock
  private TupleImpl anchorTuple;

  @Captor
  private ArgumentCaptor<Values> captor;

  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
    when(harvestedRecordsDAO.findRecord(anyString(), anyString())).thenReturn(Optional.of(
        HarvestedRecord.builder().metisDatasetId(METIS_DATASET_ID).recordLocalId(RECORD_ID)
                       .latestHarvestDate(LATEST_HARVEST_DATE).latestHarvestMd5(LATEST_HARVEST_MD5)
                       .previewHarvestDate(LATEST_HARVEST_DATE).previewHarvestMd5(LATEST_HARVEST_MD5)
                       .publishedHarvestDate(LATEST_HARVEST_DATE).publishedHarvestMd5(LATEST_HARVEST_MD5).build()));
  }

  @Test
  public void shouldRemoveRecordWithTombstoneCreating() throws Exception {
    when(indexedRecordRemover.removeRecord(any(),any(), any())).thenReturn(true);


    //when
    depublicationBolt.execute(anchorTuple, INPUT_TUPLE);

    //then
    verify(indexedRecordRemover).removeRecord(TargetIndexingDatabase.PUBLISH, RECORD_ID, REASON);
    verifyEmittedSuccessTuple();
    verify(outputCollector).ack(eq(anchorTuple));
    verifyNoMoreInteractions(outputCollector);
    verifyClearedPublishedDateAndMd5InHarvestedRecordsTable();
  }

  @Test
  public void shouldEmitErrorWhenRemoveRecordReturnedFalse() throws Exception {
    when(indexedRecordRemover.removeRecord(any(),any(), any())).thenReturn(false);
    //when
    depublicationBolt.execute(anchorTuple, INPUT_TUPLE);

    //then
    verify(indexedRecordRemover).removeRecord(TargetIndexingDatabase.PUBLISH, RECORD_ID, REASON);
    verifyEmittedErrorTuple();
    verify(outputCollector).ack(eq(anchorTuple));
    verifyNoMoreInteractions(outputCollector);
    verifyNoInteractions(harvestedRecordsDAO);
  }

  @Test
  public void shouldEmitErrorWhenRemoveRecordThrowsException() throws Exception {
    when(indexedRecordRemover.removeRecord(any(),any(), any())).thenThrow(new IndexingException("") {
    });

    //when
    depublicationBolt.execute(anchorTuple, INPUT_TUPLE);

    //then
    verify(indexedRecordRemover).removeRecord(TargetIndexingDatabase.PUBLISH, RECORD_ID, REASON);
    verifyEmittedErrorTuple();
    verify(outputCollector).ack(eq(anchorTuple));
    verifyNoMoreInteractions(outputCollector);
    verifyNoInteractions(harvestedRecordsDAO);
  }

  private void verifyClearedPublishedDateAndMd5InHarvestedRecordsTable() {
    verify(harvestedRecordsDAO).findRecord(anyString(), anyString());
    verify(harvestedRecordsDAO).insertHarvestedRecord(HarvestedRecord.builder()
                                                                     .metisDatasetId(METIS_DATASET_ID)
                                                                     .recordLocalId(RECORD_ID)
                                                                     .latestHarvestDate(LATEST_HARVEST_DATE)
                                                                     .latestHarvestMd5(LATEST_HARVEST_MD5)
                                                                     .previewHarvestDate(LATEST_HARVEST_DATE)
                                                                     .previewHarvestMd5(LATEST_HARVEST_MD5)
                                                                     .publishedHarvestDate(null)
                                                                     .publishedHarvestMd5(null).build());
  }

  private void verifyEmittedSuccessTuple() {
    verify(outputCollector).emit(eq(NOTIFICATION_STREAM_NAME), eq(anchorTuple), captor.capture());
    Values emittedTuple = captor.getValue();
    assertEquals(3, emittedTuple.size());
    assertEquals(TASK_ID, emittedTuple.get(0));
    Map<String, String> parameters = (Map<String, String>) emittedTuple.get(1);
    assertEquals(RECORD_ID, parameters.get(NotificationParameterKeys.RESOURCE));
    assertEquals(RecordState.SUCCESS.toString(), parameters.get(NotificationParameterKeys.STATE));
    assertEquals("", parameters.get(NotificationParameterKeys.INFO_TEXT));
    assertEquals("", parameters.get(NotificationParameterKeys.STATE_DESCRIPTION));
  }

  private void verifyEmittedErrorTuple() {
    verify(outputCollector).emit(eq(NOTIFICATION_STREAM_NAME), eq(anchorTuple), captor.capture());
    Values emittedTuple = captor.getValue();
    assertEquals(3, emittedTuple.size());
    assertEquals(TASK_ID, emittedTuple.get(0));
    Map<String, String> parameters = (Map<String, String>) emittedTuple.get(1);
    assertEquals(RECORD_ID, parameters.get(NotificationParameterKeys.RESOURCE));
    assertEquals(RecordState.ERROR.toString(), parameters.get(NotificationParameterKeys.STATE));
    assertNotNull(parameters.get(NotificationParameterKeys.INFO_TEXT));
    assertNotNull(parameters.get(NotificationParameterKeys.STATE_DESCRIPTION));
  }

}
