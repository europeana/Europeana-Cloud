package eu.europeana.cloud.client.dps.rest;

import co.freeside.betamax.Betamax;
import co.freeside.betamax.Recorder;
import eu.europeana.cloud.common.model.dps.*;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.DpsException;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static eu.europeana.cloud.service.dps.InputDataType.REPOSITORY_URLS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

public class DPSClientTest {

    private static final String BASE_URL_LOCALHOST = "http://localhost:8080/services/";
    private static final String BASE_URL = BASE_URL_LOCALHOST;
    private static final String USERNAME_ADMIN = "admin";
    private static final String ADMIN_PASSWORD = "ecloud_admin";
    private static final String REGULAR_USER_NAME = "user";
    private static final String REGULAR_USER_PASSWORD = "ecloud_user";
    private static final String TOPOLOGY_NAME = "TopologyName";
    private static final String NOT_DEFINED_TOPOLOGY_NAME = "NotDefinedTopologyName";
    private static final String ERROR_TYPE = "92f19f57-d173-4e07-8fc9-2a6f0df42549";
    private static final String ERROR_MESSAGE = "Message";
    private static final int ERROR_OCCURRENCES = 5;
    private static final String RESOURCE_ID = "Resource id ";
    private static final String ADDITIONAL_INFORMATIONS = "Additional informations ";
    private static final int TASK_ID = 12345;

    @Rule
    public Recorder recorder = new Recorder();

    private DpsClient dpsClient;


    @Before
    public void init() {
    }

    @Betamax(tape = "DPSClient/permitAndSubmitTask")
    @Test
    public final void shouldPermitAndSubmitTask()
            throws Exception {
        //given
        dpsClient = new DpsClient(BASE_URL, USERNAME_ADMIN, ADMIN_PASSWORD);
        DpsTask task = prepareDpsTask();

        //when
        dpsClient.topologyPermit(TOPOLOGY_NAME, REGULAR_USER_NAME);
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        long taskId = dpsClient.submitTask(task, TOPOLOGY_NAME);

        //then
        assertEquals(-2561925310040723252l, taskId);
    }

    private DpsTask prepareDpsTask() {
        DpsTask task = new DpsTask("oaiPmhHarvestingTask");
        task.addDataEntry(REPOSITORY_URLS, Arrays.asList
                ("http://example.com/oai-pmh-repository.xml"));
        task.setHarvestingDetails(new OAIPMHHarvestingDetails("Schema"));
        return task;
    }


    @Betamax(tape = "DPSClient/submitTaskAndFail")
    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public final void shouldThrowAnExceptionWhenCannotSubmitATask()
            throws Exception {
        //given
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        DpsTask task = prepareDpsTask();

        //when
        dpsClient.submitTask(task, TOPOLOGY_NAME);

        //then
        //throw an exception
    }


    @Betamax(tape = "DPSClient/permitAndSubmitTaskReturnBadURI")
    @Test(expected = RuntimeException.class)
    public final void shouldThrowAnExceptionWhenReturnedTaskIdIsNotParsable()
            throws Exception {
        //given
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        DpsTask task = prepareDpsTask();

        //when
        dpsClient.submitTask(task, TOPOLOGY_NAME);

        //then
        //throw an exception
    }

    @Test
    @Betamax(tape = "DPSClient/permitForNotDefinedTopologyTest")
    public final void shouldNotBeAbleToPermitUserForNotDefinedTopology()
            throws Exception {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        //given
        try {
            //when
            dpsClient.topologyPermit(NOT_DEFINED_TOPOLOGY_NAME, "user");
            fail();
        } catch (AccessDeniedOrTopologyDoesNotExistException e) {
            assertThat(e.getLocalizedMessage(), equalTo("The topology doesn't exist"));
        }
    }

    @Test
    @Betamax(tape = "DPSClient/getTaskProgressTest")
    public final void shouldReturnedProgressReport() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_NAME);
        TaskInfo taskInfo = new TaskInfo(TASK_ID, TOPOLOGY_NAME, TaskState.PROCESSED, "", 1, 0, 0, 0, null, null, null);
        assertThat(dpsClient.getTaskProgress(TOPOLOGY_NAME, TASK_ID), is(taskInfo));

    }

    @Test
    @Betamax(tape = "DPSClient/killTaskTest")
    public final void shouldKillTask() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_NAME);
        String responseMessage = dpsClient.killTask(TOPOLOGY_NAME, TASK_ID, null);
        assertEquals(responseMessage, "The task was killed because of Dropped by the user");

    }

    @Test
    @Betamax(tape = "DPSClient/killTaskTestWithSpecificInfo")
    public final void shouldKillTaskWithSpecificInfo() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_NAME);
        String responseMessage = dpsClient.killTask(TOPOLOGY_NAME, TASK_ID, "Aggregator-choice");
        assertEquals(responseMessage, "The task was killed because of Aggregator-choice");

    }


    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    @Betamax(tape = "DPSClient/shouldThrowExceptionWhenKillingNonExistingTaskTest")
    public final void shouldThrowExceptionWhenKillingNonExistingTaskTest() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_NAME);
        long nonExistedTaskId = 1111l;
        dpsClient.killTask(TOPOLOGY_NAME, nonExistedTaskId, null);


    }

    @Test(expected = AccessDeniedOrTopologyDoesNotExistException.class)
    @Betamax(tape = "DPSClient/shouldThrowExceptionWhenKillingTaskForNonExistedTopologyTest")
    public final void shouldThrowExceptionWhenKillingTaskForNonExistedTopologyTest() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_NAME);
        dpsClient.killTask(NOT_DEFINED_TOPOLOGY_NAME, TASK_ID, null);
    }

    @Test
    @Betamax(tape = "DPSClient_getTaskDetailsReportTest")
    public final void shouldReturnedDetailsReport() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        SubTaskInfo subTaskInfo = new SubTaskInfo(1, "resource", RecordState.SUCCESS, "", "", "result");
        List<SubTaskInfo> taskInfoList = new ArrayList<>(1);
        taskInfoList.add(subTaskInfo);
        assertThat(dpsClient.getDetailedTaskReport(TOPOLOGY_NAME, TASK_ID), is(taskInfoList));

    }

    @Test
    @Betamax(tape = "DPSClient_shouldReturnedGeneralErrorReport")
    public final void shouldReturnedGeneralErrorReport() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        TaskErrorsInfo report = createErrorInfo(TASK_ID, false);
        assertThat(dpsClient.getTaskErrorsReport(TOPOLOGY_NAME, TASK_ID, null, 0), is(report));

    }

    @Test
    @Betamax(tape = "DPSClient_shouldReturnTrueWhenErrorsReportExists")
    public final void shouldReturnTrueWhenErrorsReportExists() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        assertTrue(dpsClient.checkIfErrorReportExists(TOPOLOGY_NAME, TASK_ID));
    }

    @Test
    @Betamax(tape = "DPSClient_shouldReturnFalseWhenErrorsReportDoesNotExists")
    public final void shouldReturnFalseWhenErrorsReportDoesNotExists() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        assertFalse(dpsClient.checkIfErrorReportExists(TOPOLOGY_NAME, TASK_ID));
    }

    @Test
    @Betamax(tape = "DPSClient_shouldReturnedSpecificErrorReport")
    public final void shouldReturnedSpecificErrorReport() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        TaskErrorsInfo report = createErrorInfo(TASK_ID, true);
        assertThat(dpsClient.getTaskErrorsReport(TOPOLOGY_NAME, TASK_ID, ERROR_TYPE, 100), is(report));

    }

    @Test
    @Betamax(tape = "DPSClient_shouldReturnStatistics")
    public void shouldReturnStatistics() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        StatisticsReport report = dpsClient.getTaskStatisticsReport(TOPOLOGY_NAME, TASK_ID);
        assertNotNull(report.getNodeStatistics());
        assertEquals(TASK_ID, report.getTaskId());
    }


    @Test
    @Betamax(tape = "DPSClient_shouldReturnElementReport")
    public void shouldGetTheElementReport() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        List<NodeReport> nodeReports = dpsClient.getElementReport(TOPOLOGY_NAME, TASK_ID, "//rdf:RDF/edm:Place/skos:prefLabel");
        assertNotNull(nodeReports);
        assertEquals(1, nodeReports.size());
        assertEquals("Lattakia", nodeReports.get(0).getNodeValue());
        assertEquals(10, nodeReports.get(0).getOccurrence());
        List<AttributeStatistics> attributes = nodeReports.get(0).getAttributeStatistics();
        assertNotNull(attributes);
        assertEquals(1, attributes.size());
        assertEquals("//rdf:RDF/edm:Place/skos:prefLabel/@xml:lang", attributes.get(0).getName());
        assertEquals(10, attributes.get(0).getOccurrence());
        assertEquals("en", attributes.get(0).getValue());
    }


    @Test(expected = AccessDeniedOrTopologyDoesNotExistException.class)
    @Betamax(tape = "DPSClient_shouldThrowExceptionForStatisticsWhenTopologyDoesNotExist")
    public void shouldThrowExceptionForStatistics() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        dpsClient.getTaskStatisticsReport(NOT_DEFINED_TOPOLOGY_NAME, TASK_ID);
    }

    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    @Betamax(tape = "DPSClient_shouldThrowExceptionForStatisticsWhenTaskIdDoesNotExistOrUnAccessible")
    public void shouldThrowExceptionForStatisticsWhenTaskIdIsUnAccessible() throws DpsException {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        dpsClient.getTaskStatisticsReport(TOPOLOGY_NAME, TASK_ID);
    }


    @Test
    @Betamax(tape = "DPSClient_shouldCleanIndexingDataSet")
    public void shouldCleanIndexingDataSet() throws DpsException {
        DpsClient dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        dpsClient.cleanMetisIndexingDataset(TOPOLOGY_NAME, TASK_ID, new DataSetCleanerParameters());
    }

    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    @Betamax(tape = "DPSClient_shouldThrowAccessDeniedWhenTaskIdDoesNotExistOrUnAccessible")
    public void shouldThrowAccessDeniedWhenTaskIdDoesNotExistOrUnAccessible() throws DpsException {
        DpsClient dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        long missingTaskId = 111;
        dpsClient.cleanMetisIndexingDataset(TOPOLOGY_NAME, missingTaskId, new DataSetCleanerParameters());

    }


    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    @Betamax(tape = "DPSClient_shouldThrowAccessDeniedWhenTopologyDoesNotExist")
    public void shouldThrowAccessDeniedWhenTopologyDoesNotExist() throws DpsException {
        DpsClient dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        String wrongTopologyName = "wrongTopology";
        dpsClient.cleanMetisIndexingDataset(wrongTopologyName, TASK_ID, new DataSetCleanerParameters());
    }


    private TaskErrorsInfo createErrorInfo(long taskId, boolean specific) {
        TaskErrorsInfo info = new TaskErrorsInfo();
        info.setId(taskId);
        List<TaskErrorInfo> errors = new ArrayList<>(1);
        TaskErrorInfo error = new TaskErrorInfo();
        error.setOccurrences(ERROR_OCCURRENCES);
        error.setErrorType(ERROR_TYPE);
        error.setMessage(ERROR_MESSAGE);
        errors.add(error);
        info.setErrors(errors);

        if (specific) {
            List<ErrorDetails> errorDetails = new ArrayList<>(1);
            error.setErrorDetails(errorDetails);

            for (int i = 0; i < ERROR_OCCURRENCES; i++) {
                errorDetails.add(new ErrorDetails(RESOURCE_ID + String.valueOf(i), ADDITIONAL_INFORMATIONS + String.valueOf(i)));
            }
        }
        return info;
    }
}
