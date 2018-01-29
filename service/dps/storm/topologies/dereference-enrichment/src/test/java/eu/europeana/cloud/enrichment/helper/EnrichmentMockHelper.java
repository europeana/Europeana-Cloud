package eu.europeana.cloud.enrichment.helper;


import eu.europeana.cloud.helper.TopologyTestHelper;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.corelib.definitions.jibx.RDF;
import eu.europeana.enrichment.rest.client.EnrichmentWorker;
import eu.europeana.enrichment.utils.EnrichmentUtils;
import eu.europeana.metis.dereference.DereferenceUtils;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.when;

/**
 * Created by Tarek on 1/24/2018.
 */
public class EnrichmentMockHelper extends TopologyTestHelper {
    private EnrichmentWorker enrichmentWorker;
    protected RepresentationIterator representationIterator;

    protected void mockEnrichmentService() throws Exception {
        mockStaticEnrichmentUtils();
        enrichmentWorker = Mockito.mock(EnrichmentWorker.class);
        PowerMockito.whenNew(EnrichmentWorker.class).withAnyArguments().thenReturn(enrichmentWorker);
        when(enrichmentWorker.process(isA(RDF.class))).thenReturn(new RDF());
    }

    private void mockStaticEnrichmentUtils() throws Exception {
        PowerMockito.mockStatic(EnrichmentUtils.class);
        when(EnrichmentUtils.convertRDFtoString(isA(RDF.class))).thenReturn("Converted String");

        PowerMockito.mockStatic(DereferenceUtils.class);
        when(DereferenceUtils.toRDF(anyString())).thenReturn(new RDF());
    }

    protected void mockRepresentationIterator() throws Exception {
        representationIterator = Mockito.mock(RepresentationIterator.class);
        PowerMockito.whenNew(RepresentationIterator.class).withAnyArguments().thenReturn(representationIterator);
    }
}
