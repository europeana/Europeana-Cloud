package eu.europeana.cloud.client.dps.rest;

import co.freeside.betamax.Betamax;
import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.common.model.dps.SubTaskInfo;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import org.junit.Ignore;
import org.junit.Rule;

import co.freeside.betamax.Recorder;
import eu.europeana.cloud.service.dps.DpsTask;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class DPSClientTest {

    private static final String BASE_URL_LOCALHOST = "http://localhost:8080/services";
    private static final String BASE_URL = BASE_URL_LOCALHOST;
    private static final String USERNAME_ADMIN = "admin";
    private static final String USER_PASSWORD = "ecloud_admin";
    private static final String TOPOLOGY_NAME = "TopologyName";
    private static final String NOT_DEFINED_TOPOLOGY_NAME = "NotDefinedTopologyName";

    @Rule
    public Recorder recorder = new Recorder();

    private DpsClient dpsClient;


    @Before
    public void init() {
        dpsClient = new DpsClient(BASE_URL, USERNAME_ADMIN, USER_PASSWORD);
    }


    //@TODO ECL-520 write betamax test of dps java client
    //@Betamax(tape = "DPSClient/createAndRetrieveProviderTest")
    @Ignore
    public final void shouldSubmitTask()
            throws Exception {
        //given
        DpsTask task = new DpsTask("taskName");
        dpsClient.topologyPermit(TOPOLOGY_NAME, USER_PASSWORD);
        //when
        dpsClient.submitTask(task, TOPOLOGY_NAME);
        //then
        //this souldnt throw exception
    }

    @Test
    @Betamax(tape = "DPSClient/permitForNotDefinedTopologyTest")
    public final void shouldNotBeAbleToPermitUserForNotDefinedTopology()
            throws Exception {
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
        TaskInfo taskInfo = new TaskInfo(12345, TOPOLOGY_NAME, TaskState.PROCESSED, "", 1, 0, null, null, null);
        assertThat(dpsClient.getTaskProgress(TOPOLOGY_NAME, 12345), is(taskInfo));

    }

    @Test
    @Betamax(tape = "DPSClient_getTaskDetailsReportTest")
    public final void shouldReturnedDetailsReport() {
        SubTaskInfo subTaskInfo = new SubTaskInfo(1, "resource", States.SUCCESS, "", "", "result");
        List<SubTaskInfo> taskInfoList = new ArrayList<>();
        taskInfoList.add(subTaskInfo);
        assertThat(dpsClient.getDetailedTaskReport(TOPOLOGY_NAME, 12345), is(taskInfoList));

    }
}
