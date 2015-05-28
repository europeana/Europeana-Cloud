import org.junit.Rule;

import co.freeside.betamax.Recorder;
import eu.europeana.cloud.client.dps.rest.DpsClient;
import eu.europeana.cloud.service.dps.DpsTask;
import org.junit.Ignore;
import org.junit.Test;

public class DPSClientTest {

    @Rule
    public Recorder recorder = new Recorder();

    private static final String BASE_URL_LOCALHOST = "http://localhost:8080/dps";
    private static final String BASE_URL = BASE_URL_LOCALHOST;


    // @Betamax(tape = "DPSClient/createAndRetrieveProviderTest")
    @Test
    @Ignore
    public final void shouldSubmitTask()
            throws Exception {
        //given
        DpsClient dpsClient = new DpsClient(BASE_URL, "admin", "ecloud_admin");
        DpsTask task = new DpsTask("taskName");
        //when
        dpsClient.submitTask(task, "topologyName");

        //then

    }
}
