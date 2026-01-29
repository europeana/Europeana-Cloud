package eu.europeana.cloud.service.dps.storm.topologies.indexing.bolts;

import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.NOTIFICATION_STREAM_NAME;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexedRecordRemover;
import eu.europeana.cloud.service.dps.storm.NotificationParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.exception.IndexerRelatedIndexingException;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.tiers.model.MediaTier;
import eu.europeana.indexing.tiers.model.MetadataTier;
import eu.europeana.indexing.tiers.model.TierResults;
import eu.europeana.metis.utils.DepublicationReason;
import java.net.MalformedURLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class IndexingBoltTest {

  public static final String FILE_URL = "https://test.ecloud.psnc.pl/api/records/ZWUNIWERLFGQJUBIDPKLMSTHIDJMXC7U7LE6INQ2IZ32WHCZLHLA/representations/metadataRecord/versions/a9c549c0-88b1-11eb-b210-fa163e8d4ae3/files/ab67baa7-665f-418b-8c31-81713b0a324b";
  public static final String METIS_DATASET_ID = "exampleDS_ID";
  private static final Date EARLIER_HARVEST_DATE = new Date(1000);
  private static final UUID EARLIER_HARVEST_MD5 = UUID.randomUUID();
  private static final Date LATEST_HARVEST_DATE = new Date(2000);
  private static final UUID LATEST_HARVEST_MD5 = UUID.randomUUID();
  public static final String LOCAL_ID = "localId";
  private static final String HARVEST_DATE_TASK_PARAM = "2021-07-12T16:50:00.000Z";
  public static final Date HARVEST_DATE = DateHelper.parseISODate(HARVEST_DATE_TASK_PARAM);

  @Mock(name = "outputCollector")
  private OutputCollector outputCollector;

  @Mock
  private IndexWrapper indexWrapper;

  @Mock
  private Indexer<FullBeanImpl> indexer;

  @Mock
  private Properties indexingProperties;

  @Mock
  private IndexedRecordRemover indexedRecordRemover;

  @Mock
  private EuropeanaIdFinder europeanaIdFinder;

  @Mock
  private HarvestedRecordsDAO harvestedRecordsDAO;

  @InjectMocks
  private final IndexingBolt indexingBolt = new IndexingBolt(null, indexingProperties, "uisLocation", "user", "password");

  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
    when(indexWrapper.getIndexer(Mockito.any())).thenReturn(indexer);
  }

  @Captor
  private ArgumentCaptor<Values> captor;

  @Test
  @SuppressWarnings("unchecked")
  public void shouldIndexFileForPreviewEnv() throws Exception {
    //given
    mockEuropeanaIdFinder();
    when(harvestedRecordsDAO.findRecord(anyString(), anyString())).thenReturn(Optional.of(
        HarvestedRecord.builder().metisDatasetId(METIS_DATASET_ID).recordLocalId(LOCAL_ID)
                       .latestHarvestDate(LATEST_HARVEST_DATE).latestHarvestMd5(LATEST_HARVEST_MD5)
                       .previewHarvestDate(EARLIER_HARVEST_DATE).previewHarvestMd5(EARLIER_HARVEST_MD5)
                       .publishedHarvestDate(EARLIER_HARVEST_DATE).publishedHarvestMd5(EARLIER_HARVEST_MD5).build()));
    Tuple anchorTuple = mock(TupleImpl.class);
    String targetIndexingEnv = "PREVIEW";
    StormTaskTuple tuple = mockStormTupleFor(targetIndexingEnv);
    mockIndexer();
    //when
    indexingBolt.execute(anchorTuple, tuple);
    //then
    verify(indexer).index(anyString(), any(), any());
    Mockito.verify(outputCollector).emit(any(Tuple.class), captor.capture());
    Mockito.verify(harvestedRecordsDAO).findRecord(anyString(), anyString());
    Mockito.verify(harvestedRecordsDAO).insertHarvestedRecord(HarvestedRecord.builder()
                                                                             .metisDatasetId(METIS_DATASET_ID)
                                                                             .recordLocalId(LOCAL_ID)
                                                                             .latestHarvestDate(LATEST_HARVEST_DATE)
                                                                             .latestHarvestMd5(LATEST_HARVEST_MD5)
                                                                             .previewHarvestDate(LATEST_HARVEST_DATE)
                                                                             .previewHarvestMd5(LATEST_HARVEST_MD5)
                                                                             .publishedHarvestDate(EARLIER_HARVEST_DATE)
                                                                             .publishedHarvestMd5(EARLIER_HARVEST_MD5).build());
    Values capturedValues = captor.getValue();
    assertEquals(10, capturedValues.size());
    assertEquals(
        "https://test.ecloud.psnc.pl/api/records/ZWUNIWERLFGQJUBIDPKLMSTHIDJMXC7U7LE6INQ2IZ32WHCZLHLA/representations/metadataRecord/versions/a9c549c0-88b1-11eb-b210-fa163e8d4ae3/files/ab67baa7-665f-418b-8c31-81713b0a324b",
        capturedValues.get(2));
    Map<String, String> parameters = (Map<String, String>) capturedValues.get(4);
    assertEquals(7, parameters.size());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldIndexTier0FileInPreviewEnvironment() throws Exception {
    //given
    mockEuropeanaIdFinder();
    when(harvestedRecordsDAO.findRecord(anyString(), anyString())).thenReturn(Optional.of(
        HarvestedRecord.builder().metisDatasetId(METIS_DATASET_ID).recordLocalId(LOCAL_ID)
                       .latestHarvestDate(LATEST_HARVEST_DATE).latestHarvestMd5(LATEST_HARVEST_MD5)
                       .previewHarvestDate(EARLIER_HARVEST_DATE).previewHarvestMd5(EARLIER_HARVEST_MD5)
                       .publishedHarvestDate(EARLIER_HARVEST_DATE).publishedHarvestMd5(EARLIER_HARVEST_MD5).build()));
    Tuple anchorTuple = mock(TupleImpl.class);
    String targetIndexingEnv = "PREVIEW";
    StormTaskTuple tuple = mockStormTupleFor(targetIndexingEnv);
    mockIndexer(MediaTier.T0);
    //when
    indexingBolt.execute(anchorTuple, tuple);
    //then
    verify(indexer).index(anyString(), any(), any());
    Mockito.verify(outputCollector).emit(any(Tuple.class), captor.capture());
    Mockito.verify(harvestedRecordsDAO).findRecord(anyString(), anyString());
    Mockito.verify(harvestedRecordsDAO).insertHarvestedRecord(HarvestedRecord.builder()
                                                                             .metisDatasetId(METIS_DATASET_ID)
                                                                             .recordLocalId(LOCAL_ID)
                                                                             .latestHarvestDate(LATEST_HARVEST_DATE)
                                                                             .latestHarvestMd5(LATEST_HARVEST_MD5)
                                                                             .previewHarvestDate(LATEST_HARVEST_DATE)
                                                                             .previewHarvestMd5(LATEST_HARVEST_MD5)
                                                                             .publishedHarvestDate(EARLIER_HARVEST_DATE)
                                                                             .publishedHarvestMd5(EARLIER_HARVEST_MD5).build());
    Values capturedValues = captor.getValue();
    assertEquals(10, capturedValues.size());
    assertEquals(
        "https://test.ecloud.psnc.pl/api/records/ZWUNIWERLFGQJUBIDPKLMSTHIDJMXC7U7LE6INQ2IZ32WHCZLHLA/representations/metadataRecord/versions/a9c549c0-88b1-11eb-b210-fa163e8d4ae3/files/ab67baa7-665f-418b-8c31-81713b0a324b",
        capturedValues.get(2));
    Map<String, String> parameters = (Map<String, String>) capturedValues.get(4);
    assertEquals(7, parameters.size());
  }


  @Test
  @SuppressWarnings("unchecked")
  public void shouldIndexFilePublishEnv() throws Exception {
    //given
    mockEuropeanaIdFinder();
    when(harvestedRecordsDAO.findRecord(anyString(), anyString())).thenReturn(Optional.of(
        HarvestedRecord.builder().metisDatasetId(METIS_DATASET_ID).recordLocalId(LOCAL_ID)
                       .latestHarvestDate(LATEST_HARVEST_DATE).latestHarvestMd5(LATEST_HARVEST_MD5)
                       .previewHarvestDate(LATEST_HARVEST_DATE).previewHarvestMd5(LATEST_HARVEST_MD5)
                       .publishedHarvestDate(EARLIER_HARVEST_DATE).publishedHarvestMd5(EARLIER_HARVEST_MD5).build()));

    Tuple anchorTuple = mock(TupleImpl.class);
    String targetIndexingEnv = "PUBLISH";
    StormTaskTuple tuple = mockStormTupleFor(targetIndexingEnv);
    mockIndexer();
    //when
    indexingBolt.execute(anchorTuple, tuple);
    //then
    verify(indexer).index(anyString(), any(), any());
    Mockito.verify(outputCollector, Mockito.times(1)).emit(any(Tuple.class), captor.capture());
    Mockito.verify(harvestedRecordsDAO).findRecord(anyString(), anyString());
    Mockito.verify(harvestedRecordsDAO).insertHarvestedRecord(HarvestedRecord.builder()
                                                                             .metisDatasetId(METIS_DATASET_ID)
                                                                             .recordLocalId(LOCAL_ID)
                                                                             .latestHarvestDate(LATEST_HARVEST_DATE)
                                                                             .latestHarvestMd5(LATEST_HARVEST_MD5)
                                                                             .previewHarvestDate(LATEST_HARVEST_DATE)
                                                                             .previewHarvestMd5(LATEST_HARVEST_MD5)
                                                                             .publishedHarvestDate(LATEST_HARVEST_DATE)
                                                                             .publishedHarvestMd5(LATEST_HARVEST_MD5).build());
    Values capturedValues = captor.getValue();
    assertEquals(10, capturedValues.size());
    assertEquals(
        "https://test.ecloud.psnc.pl/api/records/ZWUNIWERLFGQJUBIDPKLMSTHIDJMXC7U7LE6INQ2IZ32WHCZLHLA/representations/metadataRecord/versions/a9c549c0-88b1-11eb-b210-fa163e8d4ae3/files/ab67baa7-665f-418b-8c31-81713b0a324b",
        capturedValues.get(2));
    Map<String, String> parameters = (Map<String, String>) capturedValues.get(4);
    assertEquals(7, parameters.size());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRemoveFileFromPublishEnvironmentIfNewVersionIsInTier0() throws Exception {
    //given
    mockEuropeanaIdFinder();
    when(harvestedRecordsDAO.findRecord(anyString(), anyString())).thenReturn(Optional.of(
        HarvestedRecord.builder().metisDatasetId(METIS_DATASET_ID).recordLocalId(LOCAL_ID)
                       .latestHarvestDate(LATEST_HARVEST_DATE).latestHarvestMd5(LATEST_HARVEST_MD5)
                       .previewHarvestDate(LATEST_HARVEST_DATE).previewHarvestMd5(LATEST_HARVEST_MD5)
                       .publishedHarvestDate(EARLIER_HARVEST_DATE).publishedHarvestMd5(EARLIER_HARVEST_MD5).build()));

    Tuple anchorTuple = mock(TupleImpl.class);
    String targetIndexingEnv = "PUBLISH";
    StormTaskTuple tuple = mockStormTupleFor(targetIndexingEnv);
    mockIndexer(MediaTier.T0);
    //when
    indexingBolt.execute(anchorTuple, tuple);
    //then
    verify(indexer).index(anyString(), any(), any());
    verify(indexedRecordRemover).removeRecord(TargetIndexingDatabase.PUBLISH, LOCAL_ID, DepublicationReason.REMOVED_DATA_AT_SOURCE);
    Mockito.verify(outputCollector).emit(any(String.class), any(Tuple.class), captor.capture());
    Mockito.verify(harvestedRecordsDAO).findRecord(anyString(), anyString());
    Mockito.verify(harvestedRecordsDAO).insertHarvestedRecord(HarvestedRecord.builder()
                                                                             .metisDatasetId(METIS_DATASET_ID)
                                                                             .recordLocalId(LOCAL_ID)
                                                                             .latestHarvestDate(LATEST_HARVEST_DATE)
                                                                             .latestHarvestMd5(LATEST_HARVEST_MD5)
                                                                             .previewHarvestDate(LATEST_HARVEST_DATE)
                                                                             .previewHarvestMd5(LATEST_HARVEST_MD5)
                                                                             .publishedHarvestDate(null).publishedHarvestMd5(null)
                                                                             .build());
    var val = (Map<String, String>) captor.getValue().get(1);
    assertEquals(
        "https://test.ecloud.psnc.pl/api/records/ZWUNIWERLFGQJUBIDPKLMSTHIDJMXC7U7LE6INQ2IZ32WHCZLHLA/representations/metadataRecord/versions/a9c549c0-88b1-11eb-b210-fa163e8d4ae3/files/ab67baa7-665f-418b-8c31-81713b0a324b",
        val.get(NotificationParameterKeys.RESOURCE));
    Assert.assertEquals(RecordState.ERROR.toString(), val.get(NotificationParameterKeys.STATE));
    Assert.assertTrue(val.get(NotificationParameterKeys.INFO_TEXT).contains("Record not suitable for publication"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRemoveDeletedRecordFromPreview() throws Exception {
    mockEuropeanaIdFinder();
    when(harvestedRecordsDAO.findRecord(anyString(), anyString())).thenReturn(Optional.of(
        HarvestedRecord.builder().metisDatasetId(METIS_DATASET_ID).recordLocalId(LOCAL_ID)
                       .latestHarvestDate(EARLIER_HARVEST_DATE).latestHarvestMd5(EARLIER_HARVEST_MD5)
                       .previewHarvestDate(EARLIER_HARVEST_DATE).previewHarvestMd5(EARLIER_HARVEST_MD5)
                       .publishedHarvestDate(EARLIER_HARVEST_DATE).publishedHarvestMd5(EARLIER_HARVEST_MD5).build()));

    Tuple anchorTuple = mock(TupleImpl.class);
    String targetIndexingEnv = "PREVIEW";
    StormTaskTuple tuple = mockStormTupleFor(targetIndexingEnv);
    tuple.setMarkedAsDeleted(true);
    mockIndexer();

    indexingBolt.execute(anchorTuple, tuple);

    verify(indexedRecordRemover).removeRecord(TargetIndexingDatabase.PREVIEW, LOCAL_ID, DepublicationReason.REMOVED_DATA_AT_SOURCE);
    Mockito.verify(outputCollector).emit(any(Tuple.class), captor.capture());
    verify(indexer, never()).index(Mockito.anyString(), Mockito.any(), any());
    verify(harvestedRecordsDAO).findRecord(anyString(), anyString());
    Mockito.verify(harvestedRecordsDAO).insertHarvestedRecord(HarvestedRecord.builder()
                                                                             .metisDatasetId(METIS_DATASET_ID)
                                                                             .recordLocalId(LOCAL_ID)
                                                                             .latestHarvestDate(EARLIER_HARVEST_DATE)
                                                                             .latestHarvestMd5(EARLIER_HARVEST_MD5)
                                                                             .previewHarvestDate(null)
                                                                             .previewHarvestMd5(null)
                                                                             .publishedHarvestDate(EARLIER_HARVEST_DATE)
                                                                             .publishedHarvestMd5(EARLIER_HARVEST_MD5)
                                                                             .build());
    Values capturedValues = captor.getValue();
    assertEquals(10, capturedValues.size());
    assertEquals(
        "https://test.ecloud.psnc.pl/api/records/ZWUNIWERLFGQJUBIDPKLMSTHIDJMXC7U7LE6INQ2IZ32WHCZLHLA/representations/metadataRecord/versions/a9c549c0-88b1-11eb-b210-fa163e8d4ae3/files/ab67baa7-665f-418b-8c31-81713b0a324b",
        capturedValues.get(2));
    Map<String, String> parameters = (Map<String, String>) capturedValues.get(4);
    assertEquals(8, parameters.size());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRemoveDeletedRecordFromPublish() throws Exception {
    mockEuropeanaIdFinder();
    when(harvestedRecordsDAO.findRecord(anyString(), anyString())).thenReturn(Optional.of(
        HarvestedRecord.builder().metisDatasetId(METIS_DATASET_ID).recordLocalId(LOCAL_ID)
                       .latestHarvestDate(EARLIER_HARVEST_DATE).latestHarvestMd5(EARLIER_HARVEST_MD5)
                       .previewHarvestDate(EARLIER_HARVEST_DATE).previewHarvestMd5(EARLIER_HARVEST_MD5)
                       .publishedHarvestDate(EARLIER_HARVEST_DATE).publishedHarvestMd5(EARLIER_HARVEST_MD5).build()));

    Tuple anchorTuple = mock(TupleImpl.class);
    String targetIndexingEnv = "PUBLISH";
    StormTaskTuple tuple = mockStormTupleFor(targetIndexingEnv);
    tuple.setMarkedAsDeleted(true);
    mockIndexer();

    indexingBolt.execute(anchorTuple, tuple);

    verify(indexedRecordRemover).removeRecord(TargetIndexingDatabase.PUBLISH, LOCAL_ID, DepublicationReason.REMOVED_DATA_AT_SOURCE);
    Mockito.verify(outputCollector).emit(any(Tuple.class), captor.capture());
    verify(indexer, never()).index(Mockito.anyString(), Mockito.any(), any());
    verify(harvestedRecordsDAO).findRecord(anyString(), anyString());
    Mockito.verify(harvestedRecordsDAO).insertHarvestedRecord(HarvestedRecord.builder()
                                                                             .metisDatasetId(METIS_DATASET_ID)
                                                                             .recordLocalId(LOCAL_ID)
                                                                             .latestHarvestDate(EARLIER_HARVEST_DATE)
                                                                             .latestHarvestMd5(EARLIER_HARVEST_MD5)
                                                                             .previewHarvestDate(EARLIER_HARVEST_DATE)
                                                                             .previewHarvestMd5(EARLIER_HARVEST_MD5)
                                                                             .publishedHarvestDate(null).publishedHarvestMd5(null)
                                                                             .build());
    Values capturedValues = captor.getValue();
    assertEquals(10, capturedValues.size());
    assertEquals(
        "https://test.ecloud.psnc.pl/api/records/ZWUNIWERLFGQJUBIDPKLMSTHIDJMXC7U7LE6INQ2IZ32WHCZLHLA/representations/metadataRecord/versions/a9c549c0-88b1-11eb-b210-fa163e8d4ae3/files/ab67baa7-665f-418b-8c31-81713b0a324b",
        capturedValues.get(2));
    Map<String, String> parameters = (Map<String, String>) capturedValues.get(4);
    assertEquals(8, parameters.size());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldInsertNewHarvestedRecordsIfNotExist() throws Exception {
    //given
    mockEuropeanaIdFinder();
    when(harvestedRecordsDAO.findRecord(anyString(), anyString())).thenReturn(Optional.empty());

    Tuple anchorTuple = mock(TupleImpl.class);
    String targetIndexingEnv = "PUBLISH";
    StormTaskTuple tuple = mockStormTupleFor(targetIndexingEnv);
    mockIndexer();
    //when
    indexingBolt.execute(anchorTuple, tuple);
    //then
    verify(indexer).index(anyString(), any(), any());
    Mockito.verify(outputCollector, Mockito.times(1)).emit(any(Tuple.class), captor.capture());
    Mockito.verify(harvestedRecordsDAO).findRecord(anyString(), anyString());
    Mockito.verify(harvestedRecordsDAO).insertHarvestedRecord(HarvestedRecord.builder()
                                                                             .metisDatasetId(METIS_DATASET_ID)
                                                                             .recordLocalId(LOCAL_ID)
                                                                             .latestHarvestDate(HARVEST_DATE)
                                                                             .publishedHarvestDate(HARVEST_DATE).build());
    Values capturedValues = captor.getValue();
    assertEquals(10, capturedValues.size());
    assertEquals(
        "https://test.ecloud.psnc.pl/api/records/ZWUNIWERLFGQJUBIDPKLMSTHIDJMXC7U7LE6INQ2IZ32WHCZLHLA/representations/metadataRecord/versions/a9c549c0-88b1-11eb-b210-fa163e8d4ae3/files/ab67baa7-665f-418b-8c31-81713b0a324b",
        capturedValues.get(2));
    Map<String, String> parameters = (Map<String, String>) capturedValues.get(4);
    assertEquals(7, parameters.size());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldEmitErrorNotificationForIndexerConfiguration() throws IndexingException {
    //given
    Tuple anchorTuple = mock(TupleImpl.class);
    StormTaskTuple tuple = mockStormTupleFor("PREVIEW");
    mockIndexer(IndexerRelatedIndexingException.class);
    //when
    indexingBolt.execute(anchorTuple, tuple);
    //then
    Mockito.verify(outputCollector).emit(any(String.class), any(Tuple.class), captor.capture());
    verify(harvestedRecordsDAO, never()).findRecord(anyString(), anyString());
    verify(harvestedRecordsDAO, never()).updatePublishedHarvestDate(anyString(), anyString(), any(Date.class));
    var val = (Map<String, String>) captor.getValue().get(1);

    assertEquals(
        "https://test.ecloud.psnc.pl/api/records/ZWUNIWERLFGQJUBIDPKLMSTHIDJMXC7U7LE6INQ2IZ32WHCZLHLA/representations/metadataRecord/versions/a9c549c0-88b1-11eb-b210-fa163e8d4ae3/files/ab67baa7-665f-418b-8c31-81713b0a324b",
        val.get(NotificationParameterKeys.RESOURCE));
    Assert.assertTrue(val.get(NotificationParameterKeys.STATE_DESCRIPTION).contains("Error while indexing"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldEmitErrorNotificationForIndexing() throws IndexingException {
    //given
    Tuple anchorTuple = mock(TupleImpl.class);
    StormTaskTuple tuple = mockStormTupleFor("PUBLISH");
    mockIndexer(IndexerRelatedIndexingException.class);
    //when
    indexingBolt.execute(anchorTuple, tuple);
    //then
    verify(outputCollector).emit(any(String.class), any(Tuple.class), captor.capture());
    verify(harvestedRecordsDAO, never()).findRecord(anyString(), anyString());
    verify(harvestedRecordsDAO, never()).updatePublishedHarvestDate(anyString(), anyString(), any(Date.class));
    var val = (Map<String, String>) captor.getValue().get(1);

    assertEquals(
        "https://test.ecloud.psnc.pl/api/records/ZWUNIWERLFGQJUBIDPKLMSTHIDJMXC7U7LE6INQ2IZ32WHCZLHLA/representations/metadataRecord/versions/a9c549c0-88b1-11eb-b210-fa163e8d4ae3/files/ab67baa7-665f-418b-8c31-81713b0a324b",
        val.get(NotificationParameterKeys.RESOURCE));
    Assert.assertTrue(val.get(NotificationParameterKeys.STATE_DESCRIPTION).contains("Error while indexing"));

  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldThrowExceptionWhenDateIsUnparsable() {
    //given
    Tuple anchorTuple = mock(TupleImpl.class);
    StormTaskTuple tuple = mockStormTupleFor("PREVIEW");
    tuple.getParameters().remove(PluginParameterKeys.METIS_RECORD_DATE);
    tuple.addParameter(PluginParameterKeys.METIS_RECORD_DATE, "UN_PARSABLE_DATE");
    //when
    indexingBolt.execute(anchorTuple, tuple);

    verify(outputCollector).emit(any(String.class), any(Tuple.class), captor.capture());
    verify(harvestedRecordsDAO, never()).findRecord(anyString(), anyString());
    verify(harvestedRecordsDAO, never()).updatePublishedHarvestDate(anyString(), anyString(), any(Date.class));
    var val = (Map<String, String>) captor.getValue().get(1);

    assertEquals(
        "https://test.ecloud.psnc.pl/api/records/ZWUNIWERLFGQJUBIDPKLMSTHIDJMXC7U7LE6INQ2IZ32WHCZLHLA/representations/metadataRecord/versions/a9c549c0-88b1-11eb-b210-fa163e8d4ae3/files/ab67baa7-665f-418b-8c31-81713b0a324b",
        val.get(NotificationParameterKeys.RESOURCE));
    Assert.assertTrue(val.get(NotificationParameterKeys.INFO_TEXT).contains("Could not parse RECORD_DATE parameter"));
    assertEquals("ERROR", val.get(NotificationParameterKeys.STATE));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionForUnknownEnv() throws IndexingException {
    //given
    Tuple anchorTuple = mock(TupleImpl.class);
    StormTaskTuple tuple = mockStormTupleFor("UNKNOWN_ENVIRONMENT");
    mockIndexer(RuntimeException.class);
    //when
    indexingBolt.execute(anchorTuple, tuple);
  }

  @Test
  public void shouldThrowExceptionWhenHarvestDateIsNull() {
    Tuple anchorTuple = mock(TupleImpl.class);
    StormTaskTuple tuple = mockStormTupleFor("PREVIEW");
    tuple.getParameters().remove(PluginParameterKeys.HARVEST_DATE);

    indexingBolt.execute(anchorTuple, tuple);

    verify(outputCollector).emit(eq(NOTIFICATION_STREAM_NAME), any(Tuple.class), captor.capture());
    verify(harvestedRecordsDAO, never()).insertHarvestedRecord(any());
    verifyNoInteractions(indexer);
  }


  @Test
  public void shouldThrowExceptionWhenHarvestDateIsUnparsable() {
    Tuple anchorTuple = mock(TupleImpl.class);
    StormTaskTuple tuple = mockStormTupleFor("PREVIEW");
    tuple.addParameter(PluginParameterKeys.HARVEST_DATE, "UN_PARSABLE_DATE");

    indexingBolt.execute(anchorTuple, tuple);

    verify(outputCollector).emit(eq(NOTIFICATION_STREAM_NAME), any(Tuple.class), captor.capture());
    verify(harvestedRecordsDAO, never()).insertHarvestedRecord(any());
    verifyNoInteractions(indexer);
  }


  private void mockEuropeanaIdFinder() throws CloudException, MalformedURLException {
    when(europeanaIdFinder.findForFileUrl(METIS_DATASET_ID, FILE_URL)).thenReturn(LOCAL_ID);
  }

  private StormTaskTuple mockStormTupleFor(final String targetDatabase) {
    //
    return new StormTaskTuple(
        1,
        "taskName",
        FILE_URL,
        new byte[]{'a', 'b', 'c'},
        new HashMap<>() {
          {
            put(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, targetDatabase);
            put(PluginParameterKeys.METIS_RECORD_DATE, DateHelper.getISODateString(new Date()));
            put(PluginParameterKeys.HARVEST_DATE, HARVEST_DATE_TASK_PARAM);
            put(PluginParameterKeys.METIS_DATASET_ID, METIS_DATASET_ID);
            put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "0");
            put(PluginParameterKeys.OUTPUT_DATA_SETS,
                "https://test.ecloud.psnc.pl/api/data-providers/metis_test5/data-sets/4979eb22-3824-4f9a-b239-edad6c4b0bb9");
          }
        }, new Revision());
  }

  private void mockIndexer(Class<? extends Throwable> throwingExceptionClass) throws IndexingException {
    doThrow(throwingExceptionClass).when(indexer).index(Mockito.anyString(), Mockito.any(), any());
  }

  private void mockIndexer() throws IndexingException {
    mockIndexer(MediaTier.T4);
  }

  private void mockIndexer(MediaTier recordMediaTier) throws IndexingException {
    doAnswer(params -> {
      Predicate<TierResults> tierResultsConsumer = params.getArgument(2);
      TierResults result = mock(TierResults.class);
      when(result.getMediaTier()).thenReturn(recordMediaTier);
      when(result.getMetadataTier()).thenReturn(MetadataTier.TC);
      tierResultsConsumer.test(result);
      return null;
    }).when(indexer).index(anyString(), any(), any());
  }
}
