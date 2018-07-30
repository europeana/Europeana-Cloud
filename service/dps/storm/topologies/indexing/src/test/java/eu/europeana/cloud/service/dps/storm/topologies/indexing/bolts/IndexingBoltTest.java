package eu.europeana.cloud.service.dps.storm.topologies.indexing.bolts;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.exception.IndexerConfigurationException;
import eu.europeana.indexing.IndexerFactory;
import eu.europeana.indexing.exception.IndexingException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

public class IndexingBoltTest {

    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;

    @Mock(name = "indexerFactoryWrapper")
    private IndexingBolt.IndexerFactoryWrapper indexerFactoryWrapper;

    @Mock
    private Indexer indexer;

    @Mock
    private IndexerFactory indexerFactory;

    @InjectMocks
    private IndexingBolt indexingBolt = new IndexingBolt(null);

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

    @Test
    public void shouldIndexFileForPreviewEnv() throws Exception {
        //given
        StormTaskTuple tuple = mockStormTupleFor("PREVIEW");
        mockIndexerFactoryFor(null);
        //when
        indexingBolt.execute(tuple);
        //then
        Mockito.verify(outputCollector, Mockito.times(1)).emit(any(Tuple.class),captor.capture());
        Values capturedValues = captor.getValue();
        Assert.assertEquals("sampleResourceUrl", capturedValues.get(2));
        Assert.assertArrayEquals(new byte[]{'a', 'b', 'c'}, (byte[]) capturedValues.get(3));
    }

    @Test
    public void shouldIndexFilePublishEnv() throws Exception {
        //given
        StormTaskTuple tuple = mockStormTupleFor("PUBLISH");
        mockIndexerFactoryFor(null);
        //when
        indexingBolt.execute(tuple);
        //then
        Mockito.verify(outputCollector, Mockito.times(1)).emit(any(Tuple.class),captor.capture());
        Values capturedValues = captor.getValue();
        Assert.assertEquals("sampleResourceUrl", capturedValues.get(2));
        Assert.assertArrayEquals(new byte[]{'a', 'b', 'c'}, (byte[]) capturedValues.get(3));
    }


    @Test
    public void shouldEmitErrorNotificationForIndexerConfiguration() throws IndexingException, IndexerConfigurationException {
        //given
        StormTaskTuple tuple = mockStormTupleFor("PREVIEW");
        mockIndexerFactoryFor(IndexerConfigurationException.class);
        //when
        indexingBolt.execute(tuple);
        //then
        Mockito.verify(outputCollector, Mockito.times(1)).emit(any(String.class),any(Tuple.class), captor.capture());
        Values capturedValues = captor.getValue();
        Map val = (Map) capturedValues.get(2);

        Assert.assertEquals("sampleResourceUrl", val.get("resource"));
        Assert.assertEquals("Error in indexer configuration", val.get("additionalInfo"));
    }

    @Test
    public void shouldEmitErrorNotificationForIOException() throws IndexerConfigurationException, IndexingException {
        //given
        StormTaskTuple tuple = mockStormTupleFor("PUBLISH");
        mockIndexerFactoryFor(IOException.class);
        //when
        indexingBolt.execute(tuple);
        //then
        Mockito.verify(outputCollector, Mockito.times(1)).emit(any(String.class),any(Tuple.class), captor.capture());
        Values capturedValues = captor.getValue();
        Map val = (Map) capturedValues.get(2);

        Assert.assertEquals("sampleResourceUrl", val.get("resource"));
        Assert.assertEquals("Error while retrieving indexer", val.get("additionalInfo"));
    }

    @Test
    public void shouldEmitErrorNotificationForIndexing() throws IndexerConfigurationException, IndexingException {
        //given
        StormTaskTuple tuple = mockStormTupleFor("PUBLISH");
        mockIndexerFactoryFor(IndexingException.class);
        //when
        indexingBolt.execute(tuple);
        //then
        Mockito.verify(outputCollector, Mockito.times(1)).emit(any(String.class),any(Tuple.class), captor.capture());
        Values capturedValues = captor.getValue();
        Map val = (Map) capturedValues.get(2);

        Assert.assertEquals("sampleResourceUrl", val.get("resource"));
        Assert.assertEquals("Error while indexing", val.get("additionalInfo"));
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionForUnknownEnv() throws IndexerConfigurationException, IndexingException {
        //given
        StormTaskTuple tuple = mockStormTupleFor("UNKNOWN_ENVIRONMENT");
        mockIndexerFactoryFor(RuntimeException.class);
        //when
        indexingBolt.execute(tuple);
    }

    private StormTaskTuple mockStormTupleFor(final String targetDatabase) {
        //
        return new StormTaskTuple(
                1,
                "taskName",
                "sampleResourceUrl",
                new byte[]{'a', 'b', 'c'},
                new HashMap<String, String>() {
                    {
                        put(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, targetDatabase);
                    }
                }, new Revision());
    }

    private void mockIndexerFactoryFor(Class clazz) throws IndexerConfigurationException, IndexingException {
        when(indexerFactoryWrapper.getIndexerFactory(Mockito.anyString(),Mockito.anyString())).thenReturn(indexerFactory);
        when(indexerFactory.getIndexer()).thenReturn(indexer);

        if (clazz != null) {
            doThrow(clazz).when(indexer).index(Mockito.anyString());
        }
    }
}
