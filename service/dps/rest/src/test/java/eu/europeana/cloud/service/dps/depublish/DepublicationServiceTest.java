package eu.europeana.cloud.service.dps.depublish;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.utils.RecordStatusUpdater;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.exception.IndexerRelatedIndexingException;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.indexing.exception.RecordRelatedIndexingException;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.net.URISyntaxException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {DepublicationService.class, DatasetDepublisher.class, TestContext.class,})
public class DepublicationServiceTest {

    private static final long TASK_ID = 1000L;
    private static final int EXPECTED_SET_SIZE = 100;
    private static final String DATASET_METIS_ID = "metisSetId";
    public static final long WAITING_FOR_COMPLETE_TIME = 7000L;
    public static final String RECORD1 = "/1/item1";
    public static final String RECORD2 = "/1/item2";

    private SubmitTaskParameters parameters;
    private DpsTask task;

    @Autowired
    private DepublicationService service;

    @Autowired
    private TaskStatusUpdater updater;

    @Autowired
    private TaskStatusChecker taskStatusChecker;

    @Autowired
    private RecordStatusUpdater recordStatusUpdater;

    @Autowired
    private MetisIndexerFactory metisIndexerFactory;

    @Mock
    private Indexer indexer;

    @Before
    public void setup() throws IndexingException, URISyntaxException {
        Mockito.reset(updater, taskStatusChecker, metisIndexerFactory, recordStatusUpdater);
        MockitoAnnotations.initMocks(this);
        task = new DpsTask();
        task.setTaskId(TASK_ID);
        task.addParameter(PluginParameterKeys.METIS_DATASET_ID, DATASET_METIS_ID);
        task.addParameter(PluginParameterKeys.RECORD_IDS_TO_DEPUBLISH, RECORD1 + "," + RECORD2);
        parameters = SubmitTaskParameters.builder().expectedSize(EXPECTED_SET_SIZE).task(task).build();
        when(metisIndexerFactory.openIndexer(anyBoolean())).thenReturn(indexer);
        when(indexer.countRecords(anyString())).thenReturn((long) EXPECTED_SET_SIZE, 0L);
        when(indexer.removeAll(anyString(), nullable(Date.class))).thenReturn(EXPECTED_SET_SIZE);
        when(indexer.remove(anyString())).thenReturn(true);
    }

    @Test
    public void verifyUseValidEnvironmentIfNoAlternativeEnvironmentParameterSet() throws IndexingException, URISyntaxException {

        service.depublishDataset(parameters);

        verify(metisIndexerFactory, atLeast(1)).openIndexer(eq(false));
        verify(metisIndexerFactory, never()).openIndexer(eq(true));
    }

    @Test
    public void verifyUseAlternativeEnvironmentIfAlternativeEnvironmentParameterSet() throws IndexingException, URISyntaxException {
        task.addParameter(PluginParameterKeys.METIS_USE_ALT_INDEXING_ENV, "true");

        service.depublishDataset(parameters);

        verify(metisIndexerFactory, atLeast(1)).openIndexer(eq(true));
        verify(metisIndexerFactory, never()).openIndexer(eq(false));
    }


    @Test
    public void verifyTaskRemoveInvokedOnIndexer() throws IndexingException {
        service.depublishDataset(parameters);

        verify(indexer).removeAll(eq(DATASET_METIS_ID), isNull());
        assertTaskSucceed();
    }


    @Test
    public void verifyValidSetSizeSavedInResults() {
        service.depublishDataset(parameters);

        verify(updater).setUpdateProcessedFiles(TASK_ID, EXPECTED_SET_SIZE, 0);
    }

    @Test
    public void verifyWaitForAllRowsRemoved() throws IndexingException {
        AtomicBoolean allRowsRemoved = new AtomicBoolean(false);
        StopWatch watch = StopWatch.createStarted();

        when(indexer.countRecords(anyString())).then(r -> {
            allRowsRemoved.set(watch.getTime() > WAITING_FOR_COMPLETE_TIME);
            if (allRowsRemoved.get()) {
                return 0L;
            } else {
                return (long) EXPECTED_SET_SIZE;
            }
        });

        service.depublishDataset(parameters);

        assertTaskSucceed();
        assertTrue(allRowsRemoved.get());
    }

    @Test
    public void verifyTaskRemoveNotInvokedIfTaskWereKilledBefore() throws IndexingException {
        when(taskStatusChecker.hasKillFlag(anyLong())).thenReturn(true);

        service.depublishDataset(parameters);

        verify(indexer, never()).removeAll(eq(DATASET_METIS_ID), isNull());
        verify(updater, never()).setTaskCompletelyProcessed(eq(TASK_ID), anyString());
    }

    @Test
    public void verifyTaskFailedWhenRemoveMethodThrowsException() throws IndexingException {
        when(indexer.removeAll(anyString(), nullable(Date.class))).thenThrow(new IndexerRelatedIndexingException("Indexer exception!"));

        service.depublishDataset(parameters);

        assertTaskFailed();
    }

    @Test
    public void verifyTaskFailedWhenRemovedRowCountNotMatchExpected() throws IndexingException {
        when(indexer.removeAll(anyString(), nullable(Date.class))).thenReturn(EXPECTED_SET_SIZE + 2);

        service.depublishDataset(parameters);

        assertTaskFailed();
    }

    //////////////////////////////record depublish///////////////////////////////////////

    @Test
    public void verifyEveryRemoveRecordInvoked() throws IndexingException {
        service.depublishIndividualRecords(parameters);

        verify(indexer).remove(RECORD1);
        verify(indexer).remove(RECORD2);
        assertTaskSucceed();
    }

    @Test
    public void verifyValidRecordCountSavedInResult() {
        service.depublishIndividualRecords(parameters);

        verify(updater).setUpdateProcessedFiles(TASK_ID, 2, 0);
        verify(recordStatusUpdater).addSuccessfullyProcessedRecord(1, TASK_ID, TopologiesNames.DEPUBLICATION_TOPOLOGY, RECORD1);
        verify(recordStatusUpdater).addSuccessfullyProcessedRecord(2, TASK_ID, TopologiesNames.DEPUBLICATION_TOPOLOGY, RECORD2);
        assertTaskSucceed();
    }

    @Test
    public void verifyRecordIsFailedInResultsWhenExceptionIsThrownWhileRemoving() throws IndexingException {
        when(indexer.remove(RECORD1)).thenThrow(RecordRelatedIndexingException.class);
        when(indexer.remove(RECORD2)).thenReturn(true);

        service.depublishIndividualRecords(parameters);

        verify(updater).setUpdateProcessedFiles(TASK_ID, 2, 1);
        verify(recordStatusUpdater).addWronglyProcessedRecord(eq(1), eq(TASK_ID), eq(TopologiesNames.DEPUBLICATION_TOPOLOGY), eq(RECORD1), any(), any());
        verify(recordStatusUpdater).addSuccessfullyProcessedRecord(2, TASK_ID, TopologiesNames.DEPUBLICATION_TOPOLOGY, RECORD2);
        assertTaskSucceed();
    }

    @Test
    public void verifyRecordIsFailedInResultsWhenRemoveRecordReturnsFalse() throws IndexingException {
        when(indexer.remove(RECORD1)).thenReturn(false);
        when(indexer.remove(RECORD2)).thenReturn(true);

        service.depublishIndividualRecords(parameters);

        verify(updater).setUpdateProcessedFiles(TASK_ID, 2, 1);
        verify(recordStatusUpdater).addWronglyProcessedRecord(eq(1), eq(TASK_ID), eq(TopologiesNames.DEPUBLICATION_TOPOLOGY), eq(RECORD1), any(), any());
        verify(recordStatusUpdater).addSuccessfullyProcessedRecord(2, TASK_ID, TopologiesNames.DEPUBLICATION_TOPOLOGY, RECORD2);
        assertTaskSucceed();
    }

    @Test
    public void verifyPerformingInterruptWhileTaskIsKilled() throws IndexingException {
        AtomicBoolean taskKilled = new AtomicBoolean(false);
        when(indexer.remove(RECORD1)).thenAnswer(invocation -> {
            taskKilled.set(true);
            return true;
        });
        when(indexer.remove(RECORD2)).thenReturn(true);
        when(taskStatusChecker.hasKillFlag(anyLong())).thenAnswer(invocation -> taskKilled.get());

        service.depublishIndividualRecords(parameters);

        verify(updater, never()).setUpdateProcessedFiles(TASK_ID, 2, 0);
        verify(recordStatusUpdater, never()).addSuccessfullyProcessedRecord(anyInt(), anyLong(), any(), eq(RECORD2));
    }

    private void assertTaskSucceed() {
        verify(updater).setTaskCompletelyProcessed(eq(TASK_ID), anyString());
    }

    private void assertTaskFailed() {
        verify(updater, never()).setTaskCompletelyProcessed(eq(TASK_ID), anyString());
        verify(updater).setTaskDropped(eq(TASK_ID), anyString());
    }

}