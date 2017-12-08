package eu.europeana.cloud.service.dps.storm.topologies.validation.topology.helper;

import eu.europeana.cloud.helper.TopologyTestHelper;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

/**
 * Created by Tarek on 12/5/2017.
 */
public class ValidationMockHelper extends TopologyTestHelper {
    protected RepresentationIterator representationIterator;

    protected void mockRepresentationIterator() throws Exception {
        representationIterator = Mockito.mock(RepresentationIterator.class);
        PowerMockito.whenNew(RepresentationIterator.class).withAnyArguments().thenReturn(representationIterator);
    }

}
