import com.lyncode.xoai.model.oaipmh.Header;
import com.lyncode.xoai.model.oaipmh.Identify;
import com.lyncode.xoai.model.oaipmh.ListIdentifiers;
import com.lyncode.xoai.serviceprovider.ServiceProvider;
import com.lyncode.xoai.serviceprovider.client.HttpOAIClient;
import com.lyncode.xoai.serviceprovider.client.OAIClient;
import com.lyncode.xoai.serviceprovider.exceptions.BadArgumentException;
import com.lyncode.xoai.serviceprovider.model.Context;
import com.lyncode.xoai.serviceprovider.parameters.ListIdentifiersParameters;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.OAIPMHSourceDetails;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.IdentifiersHarvestingBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import static eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.IdentifiersHarvestingBolt.getTestInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class IdentifiersHarvestingBoltTest {
    private IdentifiersHarvestingBolt instance;
    private OutputCollector oc;
    private final int TASK_ID = 1;
    private final String TASK_NAME = "TASK_NAME";
    private final String OAI_URL = "http://lib.psnc.pl/dlibra/oai-pmh-repository.xml";
    private final String SCHEMA = "oai_dc";

    @Before
    public void init() {
        oc = mock(OutputCollector.class);
        instance = getTestInstance(oc);
    }

    @Test
    public void testSimpleHarvesting() {
        //given
        OAIPMHSourceDetails sourceDetails = new OAIPMHSourceDetails(OAI_URL, SCHEMA);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, null, null, new HashMap<String, String>(),new Revision(), sourceDetails);
        when(oc.emit(any(Tuple.class), anyList())).thenReturn(null);
        //when
        instance.execute(tuple);
        //then
        verify(oc, atLeast(1)).emit(any(Tuple.class), captor.capture());
        assertThat(captor.getAllValues().size(), greaterThanOrEqualTo(1));
        verifyNoMoreInteractions(oc);
    }

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);
}
