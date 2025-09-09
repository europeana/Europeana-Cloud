package eu.europeana.cloud.service.dps.services.postprocessors;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.INCREMENTAL_HARVEST;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.model.dps.ProcessedRecord;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.dao.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.service.dps.storm.utils.TaskDroppedException;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.exception.IndexingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HarvestingPostProcessorTest {

  private static final long TASK_ID = 1000;
  private static final String METIS_DATASET_ID = "111";
  private static final String PROVIDER_ID = "prov";
  private static final String DATASET_ID = "datasetId";
  private static final String OUTPUT_DATA_SETS = "http://localhost:8080/mcs/data-providers/prov/data-sets/datasetId";
  private static final String RECORD_ID1 = "R1";
  private static final String RECORD_ID2 = "R2";
  private static final String CLOUD_ID1 = "a1";
  private static final String CLOUD_ID2 = "b2";
  private static final String REPRESENTATION_NAME = "repr";
  private static final String VERSION = "v1";
  private static final String RECORD1_REPRESENTATION_URI = "http://localhost:8080/mcs/records/a1/representations/repr/versions/v1";
  private static final String RECORD2_REPRESENTATION_URI = "http://localhost:8080/mcs/records/b2/representations/repr/versions/v1";
  private static final Date REVISION_TIMESTAMP = new Date(0);
  private static final String REVISION_PROVIDER = "revisionProvider";
  private static final String REVISION_NAME = "revisionName";
  private static final Revision RESULT_REVISION = new Revision(REVISION_NAME, REVISION_PROVIDER, REVISION_TIMESTAMP, true);
  private static final String HARVEST_DATE_STRING = "2021-05-26T08:00:00.000Z";
  private static final Date HARVEST_DATE = DateHelper.parseISODate(HARVEST_DATE_STRING);
  private static final Date OLDER_DATE = DateHelper.parseISODate("2021-05-26T07:30:00.000Z");

  private final DpsTask task = new DpsTask();
  private List<HarvestedRecord> allHarvestedRecords;
  private final TaskInfo taskInfo = TaskInfo.builder().build();

  @Mock
  private HarvestedRecordsDAO harvestedRecordsDAO;

  @Mock
  private ProcessedRecordsDAO processedRecordsDAO;

  @Mock
  private RecordServiceClient recordServiceClient;

  @Mock
  private RevisionServiceClient revisionServiceClient;

  @Mock
  private DataSetServiceClient dataSetServiceClient;

  @Mock
  private UISClient uisClient;

  @Mock
  private TaskStatusUpdater taskStatusUpdater;

  @Mock
  private TaskStatusChecker taskStatusChecker;

  @Mock
  private Indexer<FullBeanImpl> previewIndexer;

  @Mock
  private Indexer<FullBeanImpl> publishIndexer;

  @Mock
  private IndexWrapper indexWrapper;

  @InjectMocks
  private HarvestingPostProcessor service;

  @Before
  public void before() throws CloudException, MCSException, URISyntaxException {
    mockDAOs();
    mockUis();
    mockRecordServiceClient();
    mockIndexer();
    prepareTask();
  }

  private void mockDAOs() {
    allHarvestedRecords = new ArrayList<>();
    when(harvestedRecordsDAO.findDatasetRecords(METIS_DATASET_ID)).thenAnswer(invocation -> allHarvestedRecords.iterator());
    when(processedRecordsDAO.selectByPrimaryKey(anyLong(), anyString())).thenReturn(Optional.empty());
  }

  private void mockRecordServiceClient() throws MCSException, URISyntaxException {
    when(recordServiceClient.createRepresentation(CLOUD_ID1, REPRESENTATION_NAME, PROVIDER_ID, DATASET_ID))
        .thenReturn(new URI(RECORD1_REPRESENTATION_URI));
    when(recordServiceClient.createRepresentation(CLOUD_ID2, REPRESENTATION_NAME, PROVIDER_ID, DATASET_ID))
        .thenReturn(new URI(RECORD2_REPRESENTATION_URI));

  }

  private void mockUis() throws CloudException {
    CloudId cloudIdObject1 = createCloudId(CLOUD_ID1, RECORD_ID1);
    CloudId cloudIdObject2 = createCloudId(CLOUD_ID2, RECORD_ID2);
    when(uisClient.createCloudId(PROVIDER_ID, RECORD_ID1)).thenReturn(cloudIdObject1);
    when(uisClient.createCloudId(PROVIDER_ID, RECORD_ID2)).thenReturn(cloudIdObject2);
  }

  private void mockIndexer() {
    when(indexWrapper.getIndexer(TargetIndexingDatabase.PREVIEW)).thenReturn(previewIndexer);
    when(indexWrapper.getIndexer(TargetIndexingDatabase.PUBLISH)).thenReturn(publishIndexer);
  }

  private void prepareTask() {
    task.setTaskId(TASK_ID);
    task.addParameter(PluginParameterKeys.METIS_DATASET_ID, METIS_DATASET_ID);
    task.addParameter(PluginParameterKeys.HARVEST_DATE, HARVEST_DATE_STRING);
    task.addParameter(PluginParameterKeys.PROVIDER_ID, PROVIDER_ID);
    task.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, DATASET_ID);
    task.addParameter(PluginParameterKeys.NEW_REPRESENTATION_NAME, REPRESENTATION_NAME);
    task.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, OUTPUT_DATA_SETS);
    Revision revision = new Revision();
    revision.setRevisionName(REVISION_NAME);
    revision.setRevisionProviderId(REVISION_PROVIDER);
    revision.setCreationTimeStamp(REVISION_TIMESTAMP);
    task.setOutputRevision(revision);
  }


  @Test
  public void shouldNotFailWhenThereIsNoHarvestedRecords() {

    service.execute(taskInfo, task);

    verify(taskStatusUpdater).setTaskCompletelyProcessed(eq(TASK_ID), anyString());
  }

  @Test
  public void shouldNotDoAnythingWhenAllRecordsBelongsToCurrentHarvest() {
    allHarvestedRecords.add(createHarvestedRecord(HARVEST_DATE, RECORD_ID1));
    allHarvestedRecords.add(createHarvestedRecord(HARVEST_DATE, RECORD_ID2));

    service.execute(taskInfo, task);

    verify(taskStatusUpdater).setTaskCompletelyProcessed(eq(TASK_ID), anyString());
    verify(taskStatusUpdater).updateExpectedPostProcessedRecordsNumber(TASK_ID, 0);
    verify(taskStatusUpdater, never()).updatePostProcessedRecordsCount(anyLong(), anyInt());
    verifyNoInteractions(uisClient, recordServiceClient, revisionServiceClient, dataSetServiceClient);
  }


  @Test
  public void shouldAddOlderRecordAsDeleted() throws MCSException {
    allHarvestedRecords.add(createHarvestedRecord(OLDER_DATE, RECORD_ID1));

    service.execute(taskInfo, task);

    verify(recordServiceClient).createRepresentation(CLOUD_ID1, REPRESENTATION_NAME, PROVIDER_ID, DATASET_ID);
    verify(revisionServiceClient).addRevision(CLOUD_ID1, REPRESENTATION_NAME, VERSION, RESULT_REVISION);
    verify(processedRecordsDAO).insert(any(ProcessedRecord.class));
    verify(taskStatusUpdater, times(2))
        .updateState(eq(TASK_ID), eq(TaskState.IN_POST_PROCESSING), anyString());
    verify(taskStatusUpdater).updateExpectedPostProcessedRecordsNumber(TASK_ID, 1);
    verify(taskStatusUpdater).updatePostProcessedRecordsCount(TASK_ID, 1);
    verify(taskStatusUpdater).setTaskCompletelyProcessed(eq(TASK_ID), anyString());
    verifyNoMoreInteractions(taskStatusUpdater);
  }

  @Test
  public void shouldOmitRecordThatIsAlreadyAddedAsDeleted() {
    allHarvestedRecords.add(createHarvestedRecord(OLDER_DATE, RECORD_ID1));
    when(processedRecordsDAO.selectByPrimaryKey(TASK_ID, RECORD_ID1)).
        thenReturn(Optional.of(ProcessedRecord.builder().state(RecordState.SUCCESS).build()));

    service.execute(taskInfo, task);

    verify(taskStatusUpdater).setTaskCompletelyProcessed(eq(TASK_ID), anyString());
    verify(processedRecordsDAO, never()).insert(any());
    verify(taskStatusUpdater).updateExpectedPostProcessedRecordsNumber(TASK_ID, 1);
    verify(taskStatusUpdater, never()).updatePostProcessedRecordsCount(anyLong(), anyInt());
    verifyNoInteractions(uisClient, recordServiceClient, revisionServiceClient, dataSetServiceClient);
  }

  @Test
  public void shouldAddAllOlderRecordAsDeleted() throws MCSException {
    allHarvestedRecords.add(createHarvestedRecord(OLDER_DATE, RECORD_ID1));
    allHarvestedRecords.add(createHarvestedRecord(OLDER_DATE, RECORD_ID2));

    service.execute(taskInfo, task);

    //record1
    verify(recordServiceClient).createRepresentation(CLOUD_ID1, REPRESENTATION_NAME, PROVIDER_ID, DATASET_ID);
    verify(revisionServiceClient).addRevision(CLOUD_ID1, REPRESENTATION_NAME, VERSION, RESULT_REVISION);
    //record2
    verify(recordServiceClient).createRepresentation(CLOUD_ID2, REPRESENTATION_NAME, PROVIDER_ID, DATASET_ID);
    verify(revisionServiceClient).addRevision(CLOUD_ID2, REPRESENTATION_NAME, VERSION, RESULT_REVISION);
    //task
    verify(processedRecordsDAO, times(2)).insert(any());
    verify(taskStatusUpdater, times(2))
        .updateState(eq(TASK_ID), eq(TaskState.IN_POST_PROCESSING), anyString());
    verify(taskStatusUpdater).setTaskCompletelyProcessed(eq(TASK_ID), anyString());
    verify(taskStatusUpdater).updateExpectedPostProcessedRecordsNumber(TASK_ID, 2);
    verify(taskStatusUpdater).updatePostProcessedRecordsCount(TASK_ID, 1);
    verify(taskStatusUpdater).updatePostProcessedRecordsCount(TASK_ID, 2);
    verifyNoMoreInteractions(taskStatusUpdater);
  }

  @Test
  public void shouldNotAddRecordThatNotBelongsToCurrentHarvest() throws MCSException {
    allHarvestedRecords.add(createHarvestedRecord(HARVEST_DATE, RECORD_ID1));
    allHarvestedRecords.add(createHarvestedRecord(OLDER_DATE, RECORD_ID2));

    service.execute(taskInfo, task);

    verify(recordServiceClient).createRepresentation(CLOUD_ID2, REPRESENTATION_NAME, PROVIDER_ID, DATASET_ID);
    verify(revisionServiceClient).addRevision(CLOUD_ID2, REPRESENTATION_NAME, VERSION, RESULT_REVISION);
    verify(processedRecordsDAO).insert(any());
    verify(taskStatusUpdater, times(2))
        .updateState(eq(TASK_ID), eq(TaskState.IN_POST_PROCESSING), anyString());
    verify(taskStatusUpdater).updateExpectedPostProcessedRecordsNumber(TASK_ID, 1);
    verify(taskStatusUpdater).updatePostProcessedRecordsCount(TASK_ID, 1);
    verify(taskStatusUpdater).setTaskCompletelyProcessed(eq(TASK_ID), anyString());
    verifyNoMoreInteractions(taskStatusUpdater, recordServiceClient, revisionServiceClient, dataSetServiceClient);
  }

  @Test
  public void shouldNotStartPostprocessingForDroppedTask() {
    allHarvestedRecords.add(createHarvestedRecord(OLDER_DATE, RECORD_ID1));
    allHarvestedRecords.add(createHarvestedRecord(OLDER_DATE, RECORD_ID2));
    doThrow(new TaskDroppedException(task)).when(taskStatusChecker).checkNotDropped(any());

    assertThrows(TaskDroppedException.class, () -> service.execute(taskInfo, task));

    verifyNoInteractions(taskStatusUpdater);
    verifyNoInteractions(harvestedRecordsDAO);

  }

  @Test
  public void shouldNeedsPostProcessingReturnTrueForNoIncrementalParamSet() {

    boolean result = service.needsPostProcessing(task);

    assertTrue(result);
  }

  @Test
  public void shouldNeedsPostProcessingReturnTrueForFullHarvesting() {
    task.addParameter(INCREMENTAL_HARVEST, "false");

    boolean result = service.needsPostProcessing(task);

    assertTrue(result);
  }

  @Test
  public void shouldNeedsPostProcessingReturnTrueForIncrementalHarvesting() {
    task.addParameter(INCREMENTAL_HARVEST, "true");

    boolean result = service.needsPostProcessing(task);

    assertTrue(result);
  }

  @Test
  public void shouldCompleteHarvestedRecordsTableWithRecordsExistingInMetisPreview() throws IndexingException {
    when(previewIndexer.getRecordIds(eq(METIS_DATASET_ID), any(Date.class))).thenReturn(Stream.of(RECORD_ID1, RECORD_ID2));

    service.execute(taskInfo, task);

    verify(harvestedRecordsDAO).createUpdateIndexedColumnsIfEmptyHarvestDateStatement(eq(METIS_DATASET_ID), eq(RECORD_ID1),
        eq(TargetIndexingDatabase.PREVIEW), any(Date.class), any(UUID.class));
    verify(harvestedRecordsDAO).createUpdateIndexedColumnsIfEmptyHarvestDateStatement(eq(METIS_DATASET_ID), eq(RECORD_ID2),
        eq(TargetIndexingDatabase.PREVIEW), any(Date.class), any(UUID.class));
    verify(harvestedRecordsDAO).executeBatch(notNull());
    verify(taskStatusUpdater,times(2))
        .updateState(eq(TASK_ID), eq(TaskState.IN_POST_PROCESSING), anyString());
    verify(taskStatusUpdater).setTaskCompletelyProcessed(eq(TASK_ID), anyString());
  }

  @Test
  public void shouldCompleteHarvestedRecordsTableWithRecordsExistingInMetisPublish() throws IndexingException {
    when(publishIndexer.getRecordIds(eq(METIS_DATASET_ID), any(Date.class))).thenReturn(Stream.of(RECORD_ID1, RECORD_ID2));

    service.execute(taskInfo, task);

    verify(harvestedRecordsDAO).createUpdateIndexedColumnsIfEmptyHarvestDateStatement(eq(METIS_DATASET_ID), eq(RECORD_ID1),
        eq(TargetIndexingDatabase.PUBLISH), any(Date.class), any(UUID.class));
    verify(harvestedRecordsDAO).createUpdateIndexedColumnsIfEmptyHarvestDateStatement(eq(METIS_DATASET_ID), eq(RECORD_ID2),
        eq(TargetIndexingDatabase.PUBLISH), any(Date.class), any(UUID.class));
    verify(harvestedRecordsDAO).executeBatch(notNull());
    verify(taskStatusUpdater,times(2))
        .updateState(eq(TASK_ID), eq(TaskState.IN_POST_PROCESSING), anyString());
    verify(taskStatusUpdater).setTaskCompletelyProcessed(eq(TASK_ID), anyString());
  }

  private HarvestedRecord createHarvestedRecord(Date date, String recordId) {
    return HarvestedRecord.builder().latestHarvestDate(date).recordLocalId(recordId).previewHarvestDate(date).build();
  }

  private CloudId createCloudId(String cloudId, String localId) {
    CloudId cloudIdObject1 = new CloudId();
    cloudIdObject1.setId(cloudId);
    LocalId localIdObject = new LocalId();
    localIdObject.setRecordId(localId);
    localIdObject.setProviderId(PROVIDER_ID);
    cloudIdObject1.setLocalId(localIdObject);
    return cloudIdObject1;
  }
}