package eu.europeana.cloud.enrichment.helper;


import eu.europeana.cloud.helper.TopologyTestHelper;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.enrichment.rest.client.EnrichmentWorkerImpl;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class EnrichmentMockHelper extends TopologyTestHelper {
    protected RepresentationIterator representationIterator;

    protected void mockEnrichmentService() throws Exception {
        EnrichmentWorkerImpl enrichmentWorker = Mockito.mock(EnrichmentWorkerImpl.class);
        PowerMockito.whenNew(EnrichmentWorkerImpl.class).withAnyArguments().thenReturn(enrichmentWorker);
        when(enrichmentWorker.process(anyString())).thenReturn("Converted String");
    }

    protected void mockRepresentationIterator() throws Exception {
        representationIterator = Mockito.mock(RepresentationIterator.class);
        PowerMockito.whenNew(RepresentationIterator.class).withAnyArguments().thenReturn(representationIterator);
    }
}
