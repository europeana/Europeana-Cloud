package eu.europeana.cloud.enrichment.helper;


import eu.europeana.cloud.helper.TopologyTestHelper;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.enrichment.rest.client.EnrichmentWorker;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Created by Tarek on 1/24/2018.
 */
public class EnrichmentMockHelper extends TopologyTestHelper {
    private EnrichmentWorker enrichmentWorker;
    protected RepresentationIterator representationIterator;

    protected void mockEnrichmentService() throws Exception {
        enrichmentWorker = Mockito.mock(EnrichmentWorker.class);
        PowerMockito.whenNew(EnrichmentWorker.class).withAnyArguments().thenReturn(enrichmentWorker);
        when(enrichmentWorker.process(anyString())).thenReturn("Converted String");
    }

    protected void mockRepresentationIterator() throws Exception {
        representationIterator = Mockito.mock(RepresentationIterator.class);
        PowerMockito.whenNew(RepresentationIterator.class).withAnyArguments().thenReturn(representationIterator);
    }
}
