package eu.europeana.cloud.service.dps.storm.topologies.indexing.bolts;

//import eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts.ValidationBolt;

import eu.europeana.cloud.service.dps.storm.StormTupleKeys;
import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.IndexerConfigurationException;
import eu.europeana.indexing.IndexerFactory;
import eu.europeana.indexing.IndexingException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.io.IOException;
import java.util.Map;

import static org.mockito.Mockito.*;

public class IndexingBoltTest {

    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;

    @Mock
    private IndexerFactory factory;

    @Mock
    private Indexer indexer;

    @InjectMocks
    private IndexingBolt indexingBolt = new IndexingBolt(factory);

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

    @Test
    public void shouldIndexFile() throws Exception {
        //given
        Tuple tuple = mockStormTuple();
        mockIndexerFactoryFor(null);
        //when
        indexingBolt.execute(tuple);
        //then
        Mockito.verify(outputCollector, Mockito.times(1)).emit(Mockito.any(Tuple.class), captor.capture());
        Values capturedValues = captor.getValue();
        Assert.assertEquals(capturedValues.get(2), "sampleResourceUrl");
        Assert.assertArrayEquals((byte[]) capturedValues.get(3), new byte[]{'a', 'b', 'c'});
    }

    @Test
    public void shouldEmitErrorNotificationForIndexerConfiguration() throws IndexingException, IndexerConfigurationException {
        //given
        Tuple tuple = mockStormTuple();
        mockIndexerFactoryFor(IndexerConfigurationException.class);
        //when
        indexingBolt.execute(tuple);
        //then
        Mockito.verify(outputCollector, Mockito.times(1)).emit(any(String.class), Mockito.any(Tuple.class), captor.capture());
        Values capturedValues = captor.getValue();
        Map val = (Map) capturedValues.get(2);

        Assert.assertEquals(val.get("resource"), "sampleResourceUrl");
        Assert.assertEquals(val.get("additionalInfo"), "Error in indexer configuration");
    }

    @Test
    public void shouldEmitErrorNotificationForIOException() throws IndexerConfigurationException, IndexingException {
        //given
        Tuple tuple = mockStormTuple();
        mockIndexerFactoryFor(IOException.class);
        //when
        indexingBolt.execute(tuple);
        //then
        Mockito.verify(outputCollector, Mockito.times(1)).emit(any(String.class), Mockito.any(Tuple.class), captor.capture());
        Values capturedValues = captor.getValue();
        Map val = (Map) capturedValues.get(2);

        Assert.assertEquals(val.get("resource"), "sampleResourceUrl");
        Assert.assertEquals(val.get("additionalInfo"), "Error while retrieving indexer");
    }

    @Test
    public void shouldEmitErrorNotificationForIndexing() throws IndexerConfigurationException, IndexingException {
        //given
        Tuple tuple = mockStormTuple();
        mockIndexerFactoryFor(IndexingException.class);
        //when
        indexingBolt.execute(tuple);
        //then
        Mockito.verify(outputCollector, Mockito.times(1)).emit(any(String.class), Mockito.any(Tuple.class), captor.capture());
        Values capturedValues = captor.getValue();
        Map val = (Map) capturedValues.get(2);

        Assert.assertEquals(val.get("resource"), "sampleResourceUrl");
        Assert.assertEquals(val.get("additionalInfo"), "Error while indexing");
    }

    private Tuple mockStormTuple() {
        Tuple tuple = Mockito.mock(Tuple.class);
        when(tuple.getBinaryByField(StormTupleKeys.FILE_CONTENT_TUPLE_KEY)).thenReturn(new byte[]{'a', 'b', 'c'});
        when(tuple.getStringByField(StormTupleKeys.INPUT_FILES_TUPLE_KEY)).thenReturn("sampleResourceUrl");
        return tuple;
    }

    private void mockIndexerFactoryFor(Class clazz) throws IndexerConfigurationException, IndexingException {
        when(factory.getIndexer()).thenReturn(indexer);
        if (clazz != null) {
            doThrow(clazz).when(indexer).index(Mockito.anyString());
        }
    }
}
