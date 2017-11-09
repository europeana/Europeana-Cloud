package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helper;

import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.helper.TopologyTestHelper;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.harvester.Harvester;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helpers.SourceProvider;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;


/**
 * @author krystian.
 */
public class OAITestMocksHelper extends TopologyTestHelper {
    protected SourceProvider sourceProvider;
    protected Harvester harvester;
    protected UISClient uisClient;

    protected void mockSourceProvider() throws Exception {
        sourceProvider = Mockito.mock(SourceProvider.class);
        PowerMockito.whenNew(SourceProvider.class).withAnyArguments().thenReturn(sourceProvider);

    }

    protected void mockUISClient() throws Exception {
        uisClient = Mockito.mock(UISClient.class);
        PowerMockito.whenNew(UISClient.class).withAnyArguments().thenReturn(uisClient);

    }

    protected void mockOAIClientProvider() throws Exception {
        harvester = Mockito.mock(Harvester.class);
        PowerMockito.whenNew(Harvester.class).withAnyArguments().thenReturn(harvester);

    }

}
