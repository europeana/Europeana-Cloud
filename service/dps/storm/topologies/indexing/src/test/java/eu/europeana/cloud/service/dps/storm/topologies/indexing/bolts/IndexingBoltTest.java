package eu.europeana.cloud.service.dps.storm.topologies.indexing.bolts;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTupleKeys;
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

    @Mock(name = "indexerFactoryForPreviewEnv")
    private IndexerFactory previewFactory;

    @Mock(name = "indexerFactoryForPublishEnv")
    private IndexerFactory publishFactory;

    @Mock
    private Indexer indexer;

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
        Tuple tuple = mockStormTupleFor("PREVIEW");
        mockIndexerFactoryFor(null);
        //when
        indexingBolt.execute(tuple);
        //then
        Mockito.verify(outputCollector, Mockito.times(1)).emit(Mockito.any(Tuple.class), captor.capture());
        Values capturedValues = captor.getValue();
        Assert.assertEquals("sampleResourceUrl", capturedValues.get(2));
        Assert.assertArrayEquals(new byte[]{'a', 'b', 'c'}, (byte[]) capturedValues.get(3));
    }

    @Test
    public void shouldIndexFilePublishEnv() throws Exception {
        //given
        Tuple tuple = mockStormTupleFor("PUBLISH");
        mockIndexerFactoryFor(null);
        //when
        indexingBolt.execute(tuple);
        //then
        Mockito.verify(outputCollector, Mockito.times(1)).emit(Mockito.any(Tuple.class), captor.capture());
        Values capturedValues = captor.getValue();
        Assert.assertEquals("sampleResourceUrl", capturedValues.get(2));
        Assert.assertArrayEquals(new byte[]{'a', 'b', 'c'}, (byte[]) capturedValues.get(3));
    }


    @Test
    public void shouldEmitErrorNotificationForIndexerConfiguration() throws IndexingException, IndexerConfigurationException {
        //given
        Tuple tuple = mockStormTupleFor("PREVIEW");
        mockIndexerFactoryFor(IndexerConfigurationException.class);
        //when
        indexingBolt.execute(tuple);
        //then
        Mockito.verify(outputCollector, Mockito.times(1)).emit(any(String.class), Mockito.any(Tuple.class), captor.capture());
        Values capturedValues = captor.getValue();
        Map val = (Map) capturedValues.get(2);

        Assert.assertEquals("sampleResourceUrl", val.get("resource"));
        Assert.assertEquals("Error in indexer configuration", val.get("additionalInfo"));
    }

    @Test
    public void shouldEmitErrorNotificationForIOException() throws IndexerConfigurationException, IndexingException {
        //given
        Tuple tuple = mockStormTupleFor("PUBLISH");
        mockIndexerFactoryFor(IOException.class);
        //when
        indexingBolt.execute(tuple);
        //then
        Mockito.verify(outputCollector, Mockito.times(1)).emit(any(String.class), Mockito.any(Tuple.class), captor.capture());
        Values capturedValues = captor.getValue();
        Map val = (Map) capturedValues.get(2);

        Assert.assertEquals("sampleResourceUrl", val.get("resource"));
        Assert.assertEquals("Error while retrieving indexer", val.get("additionalInfo"));
    }

    @Test
    public void shouldEmitErrorNotificationForIndexing() throws IndexerConfigurationException, IndexingException {
        //given
        Tuple tuple = mockStormTupleFor("PUBLISH");
        mockIndexerFactoryFor(IndexingException.class);
        //when
        indexingBolt.execute(tuple);
        //then
        Mockito.verify(outputCollector, Mockito.times(1)).emit(any(String.class), Mockito.any(Tuple.class), captor.capture());
        Values capturedValues = captor.getValue();
        Map val = (Map) capturedValues.get(2);

        Assert.assertEquals("sampleResourceUrl", val.get("resource"));
        Assert.assertEquals("Error while indexing", val.get("additionalInfo"));
    }

    @Test
    public void shouldThrowExceptionForUnknownEnv() throws IndexerConfigurationException, IndexingException {
        //given
        Tuple tuple = mockStormTupleFor("UNKNOWN_ENVIRONMENT");
        mockIndexerFactoryFor(IndexingException.class);
        //when
        indexingBolt.execute(tuple);
        //then
        Mockito.verify(outputCollector, Mockito.times(1)).emit(any(String.class), Mockito.any(Tuple.class), captor.capture());
        Values capturedValues = captor.getValue();
        Map val = (Map) capturedValues.get(2);

        Assert.assertEquals("sampleResourceUrl", val.get("resource"));
        Assert.assertTrue(val.get("additionalInfo").toString().contains("RuntimeException"));
    }

    private Tuple mockStormTupleFor(final String targetDatabase) {
        Tuple tuple = Mockito.mock(Tuple.class);
        when(tuple.getBinaryByField(StormTupleKeys.FILE_CONTENT_TUPLE_KEY)).thenReturn(new byte[]{'a', 'b', 'c'});
        when(tuple.getStringByField(StormTupleKeys.INPUT_FILES_TUPLE_KEY)).thenReturn("sampleResourceUrl");
        when(tuple.getValueByField(StormTupleKeys.PARAMETERS_TUPLE_KEY)).thenReturn(
                new HashMap<String, String>() {
                    {
                        put(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, targetDatabase);
                    }
                }
        );
        return tuple;
    }

    private void mockIndexerFactoryFor(Class clazz) throws IndexerConfigurationException, IndexingException {
        when(previewFactory.getIndexer()).thenReturn(indexer);
        when(publishFactory.getIndexer()).thenReturn(indexer);
        if (clazz != null) {
            doThrow(clazz).when(indexer).index(Mockito.anyString());
        }
    }
}
