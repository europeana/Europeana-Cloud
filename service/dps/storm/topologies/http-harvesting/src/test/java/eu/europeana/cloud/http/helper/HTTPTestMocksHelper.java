package eu.europeana.cloud.http.helper;

import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.helper.TopologyTestHelper;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;


public class HTTPTestMocksHelper extends TopologyTestHelper {

    protected UISClient uisClient;

    protected void mockUISClient() throws Exception {
        uisClient = Mockito.mock(UISClient.class);
        PowerMockito.whenNew(UISClient.class).withAnyArguments().thenReturn(uisClient);

    }

}
