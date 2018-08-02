package eu.europeana.cloud;


import eu.europeana.cloud.helper.TopologyTestHelper;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.converter.Converter;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.converter.ConverterContext;
import eu.europeana.cloud.service.dps.storm.topologies.ic.topology.api.ImageConverterServiceImpl;
import eu.europeana.cloud.service.dps.storm.topologies.ic.topology.util.ImageConverterUtil;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doNothing;


/**
 * @author krystian.
 */
public class ICTestMocksHelper extends TopologyTestHelper {

    protected RepresentationIterator representationIterator;


    protected void mockImageCS() throws Exception {
        ConverterContext converterContext = Mockito.mock(ConverterContext.class);
        doNothing().when(converterContext).setConverter(any(Converter.class));
        doNothing().when(converterContext).convert(anyString(), anyString(), anyList());
        ImageConverterUtil imageConverterUtil = Mockito.mock(ImageConverterUtil.class);
        ImageConverterServiceImpl imageConverterService = new ImageConverterServiceImpl(converterContext, imageConverterUtil);
        PowerMockito.whenNew(ImageConverterServiceImpl.class).withAnyArguments().thenReturn(imageConverterService);
    }

}
