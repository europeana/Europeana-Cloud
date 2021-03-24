package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.common.model.dps.ProcessedRecord;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.RecordExecutionSubmitService;
import eu.europeana.cloud.service.dps.config.CassandraHarvestExecutorContext;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecordDAO;
import eu.europeana.cloud.service.dps.storm.utils.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.test.CassandraTestInstance;
import eu.europeana.metis.harvesting.HarvesterException;
import eu.europeana.metis.harvesting.HarvesterFactory;
import eu.europeana.metis.harvesting.ReportingIteration;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvest;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvester;
import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeader;
import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeaderIterator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@ContextConfiguration(classes = {CassandraHarvestExecutorContext.class})
@PrepareForTest({HarvesterFactory.class})
@PowerMockIgnore({"javax.management.*", "javax.security.*", "javax.net.ssl.*", "org.apache.commons.codec.digest.*"})
public class HarvestsExecutorTest {

    private static final String TOPIC = "topic_1";

    private static final Instant DATE_BEFORE_FULL = Instant.ofEpochMilli(0);
    private static final Date DATE_OF_FULL = Date.from(Instant.ofEpochMilli(1000));
    private static final Instant DATE_AFTER_FULL = Instant.ofEpochMilli(2000);
    private static final Date DATE_OF_INC1 = Date.from(Instant.ofEpochMilli(3000));
    private static final Instant DATE_AFTER_INC1 = Instant.ofEpochMilli(4000);
    private static final Date DATE_OF_INC2 = Date.from(Instant.ofEpochMilli(5000));

    private static final String OAI_ID_1 = "http://test.abc/oai/ag50034509234";
    private static final String OAI_ID_2 = "http://test.abc/oai/ag50034507777";
    private static final String OAI_ID_3 = "http://test.abc/oai/ag50034509999";
    private static final String OAI_ID_4 = "http://test.abc/oai/ag50034500000";

    public static final String DATASET_URL = "https://xyx.abc/mcs/data-providers/prov/data-sets/dat";
    public static final String DATASET_ID = "dat";
    public static final String PROVIDER_ID = "prov";

    @Rule
    public SpringClassRule springRule = new SpringClassRule();

    @Rule
    public SpringMethodRule methodRule = new SpringMethodRule();


    protected static final String KEYSPACE = "ecloud_test";
    private static final String KEYSPACE_SCHEMA_CQL = "create_dps_test_schema.cql";

    @Mock
    private OaiHarvester harvester;
    private final OaiHarvest harvest = new OaiHarvest.Builder().createOaiHarvest();

    private DpsTask task;
    private SubmitTaskParameters parameters;

    @Mock
    private OaiRecordHeaderIterator oaiIterator;

    @Autowired
    private HarvestsExecutor executor;

    @Autowired
    private RecordExecutionSubmitService recordExecutionSubmitService;

    @Autowired
    private ProcessedRecordsDAO processedRecordsDAO;

    @Autowired
    private HarvestedRecordDAO harvestedRecordDAO;

    private List<OaiRecordHeader> harvestedHeaders;
    private final List<OaiHarvest> harvestList = Collections.singletonList(harvest);

    public HarvestsExecutorTest() {
        CassandraTestInstance.getInstance(KEYSPACE_SCHEMA_CQL, KEYSPACE);
    }


    @Before
    public void setup() throws HarvesterException {
        initMocks(this);
        CassandraTestInstance.truncateAllData(false);
        mockMetisHarvestingLibrary();
        createNewTask();
    }


    @Test
    public void shouldPerformEmptyHarvest() throws HarvesterException {
        executeIncrementalHarvest(DATE_OF_INC1);

        verifyNoInteractions(recordExecutionSubmitService);
    }

    // I. New record appeared on incremental harvesting number 1

    @Test
    public void shouldPerformNewRecordOnIncrementalHarvest() throws HarvesterException {

        executeIncrementalHarvest(DATE_OF_INC1, new OaiRecordHeader(OAI_ID_1, false, DATE_AFTER_FULL));

        verifyRecordSubmitted(task.getTaskId(), OAI_ID_1);
    }

    @Test
    public void shouldNotPerformUnmodifiedRecordOnSecondIncrementalHarvesting() throws HarvesterException {
        executeIncrementalHarvest(DATE_OF_INC1, new OaiRecordHeader(OAI_ID_1, false, DATE_AFTER_FULL));
        makeRecordsIndexed(OAI_ID_1);

        executeIncrementalHarvest(DATE_OF_INC2, new OaiRecordHeader(OAI_ID_1, false, DATE_AFTER_FULL));

        verifyRecordNotSubmitted(task.getTaskId(), OAI_ID_1);
    }


    @Test
    public void shouldPerformRecordIfNotIndexedAfterPreviousIncrementalHarvesting() throws HarvesterException {
        executeIncrementalHarvest(DATE_OF_INC1, new OaiRecordHeader(OAI_ID_1, false, DATE_AFTER_FULL));

        executeIncrementalHarvest(DATE_OF_INC2, new OaiRecordHeader(OAI_ID_1, false, DATE_AFTER_FULL));

        verifyRecordSubmitted(task.getTaskId(), OAI_ID_1);
    }

    // II. Modified record on incremental harvesting number 1

    @Test
    public void shouldPerformModifiedExitingRecordOnIncrementalHarvest() throws HarvesterException {
        executeInitialFullHarvest(new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL));
        makeRecordsIndexed(OAI_ID_1);

        executeIncrementalHarvest(DATE_OF_INC1, new OaiRecordHeader(OAI_ID_1, false, DATE_AFTER_FULL));

        verifyRecordSubmitted(task.getTaskId(), OAI_ID_1);
    }

    @Test
    public void shouldNotPerformRecordExistingAndModifiedOnFirstIncrementalHarvestingOnSecondIncrementalHarvesting() throws HarvesterException {
        executeInitialFullHarvest(new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL));
        makeRecordsIndexed(OAI_ID_1);

        executeIncrementalHarvest(DATE_OF_INC1, new OaiRecordHeader(OAI_ID_1, false, DATE_AFTER_FULL));
        makeRecordsIndexed(OAI_ID_1);

        executeIncrementalHarvest(DATE_OF_INC2, new OaiRecordHeader(OAI_ID_1, false, DATE_AFTER_FULL));

        verifyRecordNotSubmitted(task.getTaskId(), OAI_ID_1);
    }

    @Test
    public void shouldPerformRecordExistingAndModifiedOnFirstIncrementalHarvestingOnSecondIncrementalHarvestingIfNotIndexedAfterFirst() throws HarvesterException {
        executeInitialFullHarvest(new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL));
        makeRecordsIndexed(OAI_ID_1);

        executeIncrementalHarvest(DATE_OF_INC1, new OaiRecordHeader(OAI_ID_1, false, DATE_AFTER_FULL));

        executeIncrementalHarvest(DATE_OF_INC2, new OaiRecordHeader(OAI_ID_1, false, DATE_AFTER_FULL));

        verifyRecordSubmitted(task.getTaskId(), OAI_ID_1);
    }

    /// III. Record modified on incremental harvesting number 1 but is not indexed after full harvest
    @Test
    public void shouldPerformModifiedNonExitingRecordOnIncrementalHarvest() throws HarvesterException {
        executeInitialFullHarvest(new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL));

        executeIncrementalHarvest(DATE_OF_INC1, new OaiRecordHeader(OAI_ID_1, false, DATE_AFTER_FULL));

        verifyRecordSubmitted(task.getTaskId(), OAI_ID_1);
    }

    @Test
    public void shouldNotPerformRecordNonExistingAndModifiedOnFirstIncrementalHarvestingOnSecondIncrementalHarvesting() throws HarvesterException {
        executeInitialFullHarvest(new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL));

        executeIncrementalHarvest(DATE_OF_INC1, new OaiRecordHeader(OAI_ID_1, false, DATE_AFTER_FULL));
        makeRecordsIndexed(OAI_ID_1);

        executeIncrementalHarvest(DATE_OF_INC2, new OaiRecordHeader(OAI_ID_1, false, DATE_AFTER_FULL));

        verifyRecordNotSubmitted(task.getTaskId(), OAI_ID_1);
    }

    @Test
    public void shouldPerformRecordNonExistingAndModifiedOnFirstIncrementalHarvestingOnSecondIncrementalHarvestingIfNotIndexedAfterFirst() throws HarvesterException {
        executeInitialFullHarvest(new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL));

        executeIncrementalHarvest(DATE_OF_INC1, new OaiRecordHeader(OAI_ID_1, false, DATE_AFTER_FULL));

        executeIncrementalHarvest(DATE_OF_INC2, new OaiRecordHeader(OAI_ID_1, false, DATE_AFTER_FULL));

        verifyRecordSubmitted(task.getTaskId(), OAI_ID_1);
    }

    // IV. Record exist after full harvest and is modified on incremental harvest 1 and 2

    @Test
    public void shouldPerformModificationsOfExistingRecordOnTwoSubsequentHarvests() throws HarvesterException {
        executeInitialFullHarvest(new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL));
        makeRecordsIndexed(OAI_ID_1);

        executeIncrementalHarvest(DATE_OF_INC1, new OaiRecordHeader(OAI_ID_1, false, DATE_AFTER_FULL));
        verifyRecordSubmitted(task.getTaskId(), OAI_ID_1);
        makeRecordsIndexed(OAI_ID_1);

        executeIncrementalHarvest(DATE_OF_INC2, new OaiRecordHeader(OAI_ID_1, false, DATE_AFTER_INC1));

        verifyRecordSubmitted(task.getTaskId(), OAI_ID_1);
    }

    @Test
    public void shouldPerformModifiedExistingRecordOnSecondIncrementalHarvestingEvenIfNotIndexedOnFirst() throws HarvesterException {
        executeInitialFullHarvest(new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL));
        makeRecordsIndexed(OAI_ID_1);

        executeIncrementalHarvest(DATE_OF_INC1, new OaiRecordHeader(OAI_ID_1, false, DATE_AFTER_FULL));

        executeIncrementalHarvest(DATE_OF_INC2, new OaiRecordHeader(OAI_ID_1, false, DATE_AFTER_INC1));

        verifyRecordSubmitted(task.getTaskId(), OAI_ID_1);
    }

    // V.
    @Test
    public void shouldNotPerformUnmodifiedExistingRecord() throws HarvesterException {
        executeInitialFullHarvest(new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL));
        makeRecordsIndexed(OAI_ID_1);

        executeIncrementalHarvest(DATE_OF_INC1, new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL));

        verifyRecordNotSubmitted(task.getTaskId(), OAI_ID_1);

        executeIncrementalHarvest(DATE_OF_INC2, new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL));

        verifyRecordNotSubmitted(task.getTaskId(), OAI_ID_1);
    }

    //VI.  Unmodified existing record od inc1 but modified on inc2
    @Test
    public void shouldPerformExistingRecordIfModifiedOnInc2ButNotModifiedOnInc1() throws HarvesterException {
        executeInitialFullHarvest(new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL));
        makeRecordsIndexed(OAI_ID_1);

        executeIncrementalHarvest(DATE_OF_INC1, new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL));

        verifyRecordNotSubmitted(task.getTaskId(), OAI_ID_1);

        executeIncrementalHarvest(DATE_OF_INC2, new OaiRecordHeader(OAI_ID_1, false, DATE_AFTER_INC1));

        verifyRecordSubmitted(task.getTaskId(), OAI_ID_1);
    }


    //VII. Unmodified nonexistent record on inc1
    @Test
    public void shouldPerformUnmodifiedNonExistingRecordOnEveryIncrementalHarvest() throws HarvesterException {
        executeInitialFullHarvest(new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL));

        executeIncrementalHarvest(DATE_OF_INC1, new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL));
        verifyRecordSubmitted(task.getTaskId(), OAI_ID_1);

        executeIncrementalHarvest(DATE_OF_INC1, new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL));
        verifyRecordSubmitted(task.getTaskId(), OAI_ID_1);
    }
//VIII. Delete existing record on inc

    @Test
    public void shouldDeleteRecordNotOnHarvestList() throws HarvesterException {
        executeInitialFullHarvest(new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL));
        makeRecordsIndexed(OAI_ID_1);

        executeIncrementalHarvest(DATE_OF_INC1);
        verifyRecordSubmittedToDelete(task.getTaskId(), OAI_ID_1);
    }

    @Test
    public void shouldDeleteRecordMarkedAsDeletedOnHarvestList() throws HarvesterException {
        executeInitialFullHarvest(new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL));
        makeRecordsIndexed(OAI_ID_1);

        executeIncrementalHarvest(DATE_OF_INC1, new OaiRecordHeader(OAI_ID_1, true, DATE_BEFORE_FULL));
        verifyRecordSubmittedToDelete(task.getTaskId(), OAI_ID_1);
    }

    @Test
    public void shouldDeleteRecordNotExistingOnHarvestListOnSecondTry() throws HarvesterException {
        executeInitialFullHarvest(new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL));
        makeRecordsIndexed(OAI_ID_1);

        executeIncrementalHarvest(DATE_OF_INC1);
        verifyRecordSubmittedToDelete(task.getTaskId(), OAI_ID_1);

        executeIncrementalHarvest(DATE_OF_INC2);
        verifyRecordSubmittedToDelete(task.getTaskId(), OAI_ID_1);
    }

    @Test
    public void shouldDeleteRecordMarkedAsDeletedOnSecondTry() throws HarvesterException {
        executeInitialFullHarvest(new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL));
        makeRecordsIndexed(OAI_ID_1);

        executeIncrementalHarvest(DATE_OF_INC1, new OaiRecordHeader(OAI_ID_1, true, DATE_BEFORE_FULL));
        verifyRecordSubmittedToDelete(task.getTaskId(), OAI_ID_1);

        executeIncrementalHarvest(DATE_OF_INC1, new OaiRecordHeader(OAI_ID_1, true, DATE_BEFORE_FULL));
        verifyRecordSubmittedToDelete(task.getTaskId(), OAI_ID_1);
    }

    //IX. Delete existing record oni inc2
    @Test
    public void shouldDeleteRecordNotOnHarvestListInIncrementalHarvest2() throws HarvesterException {
        executeInitialFullHarvest(new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL));
        makeRecordsIndexed(OAI_ID_1);

        executeIncrementalHarvest(DATE_OF_INC1, new OaiRecordHeader(OAI_ID_1, false, DATE_AFTER_FULL));
        makeRecordsIndexed(OAI_ID_1);

        executeIncrementalHarvest(DATE_OF_INC2);

        verifyRecordSubmittedToDelete(task.getTaskId(), OAI_ID_1);
    }

    @Test
    public void shouldDeleteRecordMarkedAsDeletedInIncrementalHarvest2() throws HarvesterException {
        executeInitialFullHarvest(new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL));
        makeRecordsIndexed(OAI_ID_1);

        executeIncrementalHarvest(DATE_OF_INC1, new OaiRecordHeader(OAI_ID_1, false, DATE_AFTER_FULL));
        makeRecordsIndexed(OAI_ID_1);

        executeIncrementalHarvest(DATE_OF_INC2, new OaiRecordHeader(OAI_ID_1, true, DATE_AFTER_FULL));

        verifyRecordSubmittedToDelete(task.getTaskId(), OAI_ID_1);
    }

    //X. Delete of nonexistent old record (not in harvested record)
    @Test
    public void shouldIgnoreRecordThatIsMarkedAsDeletedButIsNotInSystem() throws HarvesterException {
        executeIncrementalHarvest(DATE_OF_INC1, new OaiRecordHeader(OAI_ID_1, true, DATE_BEFORE_FULL));

        verifyRecordNotSubmitted(task.getTaskId(), OAI_ID_1);
    }
    //XI. Delete record that is not indexed
    @Test
    public void shouldIgnoreRecordThatIsNotOnHarvestedListButIsNotIndexed() throws HarvesterException {
        executeInitialFullHarvest(new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL));

        executeIncrementalHarvest(DATE_OF_INC1);

        verifyRecordNotSubmitted(task.getTaskId(), OAI_ID_1);
    }

    @Test
    public void shouldIgnoreRecordThatIsMarkedAsDeletedButIsNotIndexed() throws HarvesterException {
        executeInitialFullHarvest(new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL));

        executeIncrementalHarvest(DATE_OF_INC1, new OaiRecordHeader(OAI_ID_1, true, DATE_BEFORE_FULL));

        verifyRecordNotSubmitted(task.getTaskId(), OAI_ID_1);
    }

    //XII. Existing Record delete on Inc1 and Create on int2
    @Test
    public void shouldRecreateRecordThatWasDeletedOnPreviousIncrementalHarvest() throws HarvesterException {
        executeInitialFullHarvest(new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL));
        makeRecordsIndexed(OAI_ID_1);

        executeIncrementalHarvest(DATE_OF_INC1);
        verifyRecordSubmittedToDelete(task.getTaskId(), OAI_ID_1);
        makeRecordsDeletedFromIndex(OAI_ID_1);

        executeIncrementalHarvest(DATE_OF_INC2, new OaiRecordHeader(OAI_ID_1, true, DATE_AFTER_INC1));
        verifyRecordSubmitted(task.getTaskId(), OAI_ID_1);
    }

    ////////////////// Many record scenario /////////////////////
    @Test
    public void shouldPerformValidManyRecordsWithDifferentChangesRecord() throws HarvesterException {
        executeInitialFullHarvest(
                new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL),
                new OaiRecordHeader(OAI_ID_2, false, DATE_BEFORE_FULL),
                new OaiRecordHeader(OAI_ID_3, false, DATE_BEFORE_FULL));
        makeRecordsIndexed(OAI_ID_1, OAI_ID_2, OAI_ID_3);

        executeIncrementalHarvest(DATE_OF_INC1,
                new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL),
                new OaiRecordHeader(OAI_ID_2, false, DATE_AFTER_FULL),
                new OaiRecordHeader(OAI_ID_4, false, DATE_AFTER_FULL));

        verifyRecordNotSubmitted(task.getTaskId(), OAI_ID_1);
        verifyRecordSubmitted(task.getTaskId(), OAI_ID_2);
        verifyRecordSubmittedToDelete(task.getTaskId(), OAI_ID_3);
        verifyRecordSubmitted(task.getTaskId(), OAI_ID_4);

        makeRecordsIndexed(OAI_ID_1, OAI_ID_2);
        makeRecordsDeletedFromIndex(OAI_ID_3);

        executeIncrementalHarvest(DATE_OF_INC2,
                new OaiRecordHeader(OAI_ID_1, false, DATE_BEFORE_FULL),
                new OaiRecordHeader(OAI_ID_2, false, DATE_AFTER_FULL),
                new OaiRecordHeader(OAI_ID_4, false, DATE_AFTER_FULL));
        verifyRecordNotSubmitted(task.getTaskId(), OAI_ID_1);
        verifyRecordNotSubmitted(task.getTaskId(), OAI_ID_2);
        verifyRecordNotSubmitted(task.getTaskId(), OAI_ID_3);
        verifyRecordSubmitted(task.getTaskId(), OAI_ID_4);
    }

    private void executeInitialFullHarvest(OaiRecordHeader... headers) throws HarvesterException {
        harvestedHeaders = Arrays.asList(headers);
        parameters.setCurrentHarvestDate(DATE_OF_FULL);
        executor.execute(harvestList, parameters);
    }

    private void executeIncrementalHarvest(Date harvestDate, OaiRecordHeader... headers) throws HarvesterException {
        createNewTask();
        Mockito.clearInvocations(recordExecutionSubmitService);
        task.addParameter(PluginParameterKeys.INCREMENTAL_HARVEST, "true");
        parameters.setCurrentHarvestDate(harvestDate);
        harvestedHeaders = Arrays.asList(headers);
        executor.execute(harvestList, parameters);
    }


    private void createNewTask() {
        task = new DpsTask();
        task.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, DATASET_URL);
        parameters = SubmitTaskParameters.builder().task(task).topicName(TOPIC).build();
    }

    private void makeRecordsIndexed(String... recordIds) {
        //simulate row is indexed
        for (String recordId : recordIds) {
            HarvestedRecord record = harvestedRecordDAO.findRecord(PROVIDER_ID, DATASET_ID, recordId).orElseThrow();
            harvestedRecordDAO.updateIndexingFields(PROVIDER_ID, DATASET_ID, recordId, new Date(), record.getHarvestDate());
        }
    }

    private void makeRecordsDeletedFromIndex(String recordId) {
        //simulate row is deleted from index
        assertFalse(harvestedRecordDAO.findRecord(PROVIDER_ID, DATASET_ID, recordId).isEmpty());
        harvestedRecordDAO.deleteRecord(PROVIDER_ID, DATASET_ID, recordId);
        assertTrue(harvestedRecordDAO.findRecord(PROVIDER_ID, DATASET_ID, recordId).isEmpty());
    }


    private void verifyRecordSubmitted(long taskId, String recordId) {
        verify(recordExecutionSubmitService).submitRecord(
                argThat(samePropertyValuesAs(DpsRecord.builder().taskId(taskId).recordId(recordId).build()))
                , eq(TOPIC));

        Optional<ProcessedRecord> processedRecord = processedRecordsDAO.selectByPrimaryKey(taskId, recordId);
        assertEquals(RecordState.QUEUED, processedRecord.orElseThrow().getState());
    }

    private void verifyRecordSubmittedToDelete(long taskId, String recordId) {
        verify(recordExecutionSubmitService).submitRecord(
                argThat(samePropertyValuesAs(DpsRecord.builder().taskId(taskId).recordId(recordId).markedAsDeleted(true).build()))
                , eq(TOPIC));

        Optional<ProcessedRecord> processedRecord = processedRecordsDAO.selectByPrimaryKey(taskId, recordId);
        assertEquals(RecordState.QUEUED, processedRecord.orElseThrow().getState());
    }


    private void verifyRecordNotSubmitted(long taskId, String recordId) {
        verify(recordExecutionSubmitService, never()).submitRecord(
                argThat(hasProperty("recordId", equalTo(recordId))),
                any());
        assertTrue(processedRecordsDAO.selectByPrimaryKey(taskId, recordId).isEmpty());
    }

    private void mockMetisHarvestingLibrary() throws HarvesterException {
        mockStatic(HarvesterFactory.class);
        PowerMockito.when(HarvesterFactory.createOaiHarvester(any(), anyInt(), anyInt())).thenReturn(harvester);
        when(harvester.harvestRecordHeaders(any(OaiHarvest.class))).thenReturn(oaiIterator);
        doAnswer(invocation -> {
            ReportingIteration<OaiRecordHeader> action = invocation.getArgument(0);
            for (OaiRecordHeader header : harvestedHeaders) {
                action.process(header);
            }
            return null;
        }).when(oaiIterator).forEach(any());
    }
}