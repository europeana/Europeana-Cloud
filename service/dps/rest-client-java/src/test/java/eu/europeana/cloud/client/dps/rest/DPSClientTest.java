package eu.europeana.cloud.client.dps.rest;

import co.freeside.betamax.Betamax;
import eu.europeana.cloud.common.model.dps.*;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import org.glassfish.jersey.Beta;
import org.junit.Ignore;
import org.junit.Rule;

import co.freeside.betamax.Recorder;
import eu.europeana.cloud.service.dps.DpsTask;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static eu.europeana.cloud.service.dps.InputDataType.REPOSITORY_URLS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
    @Test(expected = RuntimeException.class )
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
    @Test(expected = RuntimeException.class )
    public final void shouldThrowAnExceptionWhenURIMalformed()
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
        } catch (RuntimeException e) {
            //then
            assertThat(e.getLocalizedMessage(), equalTo("Permit topology failed!"));
        }
    }

    @Test
    @Betamax(tape = "DPSClient/getTaskProgressTest")
    public final void shouldReturnedProgressReport() {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_NAME);
        TaskInfo taskInfo = new TaskInfo(TASK_ID, TOPOLOGY_NAME, TaskState.PROCESSED, "", 1, 0, 0, null, null, null);
        assertThat(dpsClient.getTaskProgress(TOPOLOGY_NAME, TASK_ID), is(taskInfo));

    }

    @Test
    @Betamax(tape = "DPSClient_getTaskDetailsReportTest")
    public final void shouldReturnedDetailsReport() {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        SubTaskInfo subTaskInfo = new SubTaskInfo(1, "resource", States.SUCCESS, "", "", "result");
        List<SubTaskInfo> taskInfoList = new ArrayList<>();
        taskInfoList.add(subTaskInfo);
        assertThat(dpsClient.getDetailedTaskReport(TOPOLOGY_NAME, TASK_ID), is(taskInfoList));

    }

    @Test
    @Betamax(tape = "DPSClient_shouldReturnedGeneralErrorReport")
    public final void shouldReturnedGeneralErrorReport() {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        TaskErrorsInfo report = createErrorInfo(TASK_ID, false);
        assertThat(dpsClient.getTaskErrorsReport(TOPOLOGY_NAME, TASK_ID, null), is(report));

    }

    @Test
    @Betamax(tape = "DPSClient_shouldReturnedSpecificErrorReport")
    public final void shouldReturnedSpecificErrorReport() {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        TaskErrorsInfo report = createErrorInfo(TASK_ID, true);
        assertThat(dpsClient.getTaskErrorsReport(TOPOLOGY_NAME, TASK_ID, ERROR_TYPE), is(report));

    }

    @Test
    @Betamax(tape ="DPSClient_shouldReturnStatistics" )
    public void shouldReturnStatistics() {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        StatisticsReport expected = new StatisticsReport(TASK_ID);
        assertThat(dpsClient.getTaskStatisticsReport(TOPOLOGY_NAME, TASK_ID), is(expected));
    }

    private TaskErrorsInfo createErrorInfo(long taskId, boolean specific) {
        TaskErrorsInfo info = new TaskErrorsInfo();
        info.setId(taskId);
        List<TaskErrorInfo> errors = new ArrayList<>();
        TaskErrorInfo error = new TaskErrorInfo();
        error.setOccurrences(ERROR_OCCURRENCES);
        error.setErrorType(ERROR_TYPE);
        error.setMessage(ERROR_MESSAGE);
        errors.add(error);
        info.setErrors(errors);

        if (specific) {
            List<String> identifiers = new ArrayList<>();
            error.setIdentifiers(identifiers);

            for (int i = 0; i < ERROR_OCCURRENCES; i++) {
                identifiers.add(RESOURCE_ID + String.valueOf(i));
            }
        }
        return info;
    }
}
