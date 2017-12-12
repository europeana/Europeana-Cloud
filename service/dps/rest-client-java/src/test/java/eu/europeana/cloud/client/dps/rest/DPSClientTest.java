package eu.europeana.cloud.client.dps.rest;

import co.freeside.betamax.Betamax;
import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.common.model.dps.SubTaskInfo;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import org.junit.Rule;

import co.freeside.betamax.Recorder;
import eu.europeana.cloud.service.dps.DpsTask;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static eu.europeana.cloud.service.dps.InputDataType.REPOSITORY_URLS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
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
        DpsTask task = new DpsTask("oaiPmhHarvestingTask");
        task.addDataEntry(REPOSITORY_URLS, Arrays.asList
                ("http://example.com/oai-pmh-repository.xml"));
        task.setHarvestingDetails(new OAIPMHHarvestingDetails("Schema"));

        //when
        dpsClient.topologyPermit(TOPOLOGY_NAME, REGULAR_USER_NAME);
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        URI location = dpsClient.submitTask(task, TOPOLOGY_NAME);

        //then
        assertEquals(URI.create("http://localhost:8080/services/TopologyName/tasks/2561925310040723252"), location);
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
        TaskInfo taskInfo = new TaskInfo(12345, TOPOLOGY_NAME, TaskState.PROCESSED, "", 1, 0, null, null, null);
        assertThat(dpsClient.getTaskProgress(TOPOLOGY_NAME, 12345), is(taskInfo));

    }

    @Test
    @Betamax(tape = "DPSClient_getTaskDetailsReportTest")
    public final void shouldReturnedDetailsReport() {
        dpsClient = new DpsClient(BASE_URL, REGULAR_USER_NAME, REGULAR_USER_PASSWORD);
        SubTaskInfo subTaskInfo = new SubTaskInfo(1, "resource", States.SUCCESS, "", "", "result");
        List<SubTaskInfo> taskInfoList = new ArrayList<>();
        taskInfoList.add(subTaskInfo);
        assertThat(dpsClient.getDetailedTaskReport(TOPOLOGY_NAME, 12345), is(taskInfoList));

    }
}
