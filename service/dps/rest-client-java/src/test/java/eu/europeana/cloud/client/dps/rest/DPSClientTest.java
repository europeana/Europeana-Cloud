package eu.europeana.cloud.client.dps.rest;

import co.freeside.betamax.Betamax;
import org.junit.Ignore;
import org.junit.Rule;

import co.freeside.betamax.Recorder;
import eu.europeana.cloud.service.dps.DpsTask;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class DPSClientTest {

    private static final String BASE_URL_LOCALHOST = "http://localhost:8080/dps";
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
    @Test
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
        }catch (RuntimeException e){
            //then
            assertThat(e.getLocalizedMessage(), equalTo("Permit topology failed!"));
        }
    }
}
