package eu.europeana.cloud.service.dps.storm.topologies.indexing.bolts;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.*;

import com.google.gson.Gson;
import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.indexing.bolts.IndexingBolt.IndexerPoolWrapper;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.indexing.IndexerPool;
import eu.europeana.indexing.exception.IndexerRelatedIndexingException;
import eu.europeana.indexing.exception.IndexingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class IndexingBoltTest {

    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;

    @Mock(name = "indexerPoolWrapper")
    private IndexerPoolWrapper indexerPoolWrapper;

    @Mock
    private IndexerPool indexerPool;

    @Mock
    private Properties indexingProperties;

    @Mock
    private UISClient uisClient;

    @Mock
    private HarvestedRecordsDAO harvestedRecordsDAO;

    @InjectMocks
    private IndexingBolt indexingBolt = new IndexingBolt(null, indexingProperties, "uisLocation");

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

    @Test
    public void shouldIndexFileForPreviewEnv() throws Exception {
        //given
        mockUisClient();
        when(harvestedRecordsDAO.findRecord(anyString(), anyString())).thenReturn(Optional.of(HarvestedRecord.builder().build()));
        Tuple anchorTuple = mock(TupleImpl.class);
        String targetIndexingEnv = "PREVIEW";
        StormTaskTuple tuple = mockStormTupleFor(targetIndexingEnv);
        mockIndexerFactoryFor(null);
        //when
        indexingBolt.execute(anchorTuple, tuple);
        //then
        verify(indexerPool).index(anyString(), any());
        Mockito.verify(outputCollector).emit(any(Tuple.class), captor.capture());
        Mockito.verify(harvestedRecordsDAO).findRecord(anyString(),anyString());
        Mockito.verify(harvestedRecordsDAO).updatePublishingDate(eq("exampleDS_ID"), eq("localId"), any(Date.class));
        Values capturedValues = captor.getValue();
        assertEquals(8, capturedValues.size());
        assertEquals("https://test.ecloud.psnc.pl/api/records/ZWUNIWERLFGQJUBIDPKLMSTHIDJMXC7U7LE6INQ2IZ32WHCZLHLA/representations/metadataRecord/versions/a9c549c0-88b1-11eb-b210-fa163e8d4ae3/files/ab67baa7-665f-418b-8c31-81713b0a324b", capturedValues.get(2));
        Map<String, String> parameters = (Map<String, String>) capturedValues.get(4);
        assertEquals(6, parameters.size());
        DataSetCleanerParameters dataSetCleanerParameters = new Gson().fromJson(parameters.get(PluginParameterKeys.DATA_SET_CLEANING_PARAMETERS), DataSetCleanerParameters.class);
        assertFalse(dataSetCleanerParameters.isUsingAltEnv());
        assertEquals(targetIndexingEnv, dataSetCleanerParameters.getTargetIndexingEnv());
    }

    @Test
    public void shouldIndexFilePublishEnv() throws Exception {
        //given
        mockUisClient();
        when(harvestedRecordsDAO.findRecord(anyString(),anyString())).thenReturn(Optional.of(HarvestedRecord.builder().build()));
        Tuple anchorTuple = mock(TupleImpl.class);
        String targetIndexingEnv = "PUBLISH";
        StormTaskTuple tuple = mockStormTupleFor(targetIndexingEnv);
        mockIndexerFactoryFor(null);
        //when
        indexingBolt.execute(anchorTuple, tuple);
        //then
        verify(indexerPool).index(anyString(), any());
        Mockito.verify(outputCollector, Mockito.times(1)).emit(any(Tuple.class), captor.capture());
        Mockito.verify(harvestedRecordsDAO).findRecord(anyString(),anyString());
        Mockito.verify(harvestedRecordsDAO).updatePublishingDate(eq("exampleDS_ID"), eq("localId"), any(Date.class));

        Values capturedValues = captor.getValue();
        assertEquals(8, capturedValues.size());
        assertEquals("https://test.ecloud.psnc.pl/api/records/ZWUNIWERLFGQJUBIDPKLMSTHIDJMXC7U7LE6INQ2IZ32WHCZLHLA/representations/metadataRecord/versions/a9c549c0-88b1-11eb-b210-fa163e8d4ae3/files/ab67baa7-665f-418b-8c31-81713b0a324b", capturedValues.get(2));
        Map<String, String> parameters = (Map<String, String>) capturedValues.get(4);
        assertEquals(6, parameters.size());
        DataSetCleanerParameters dataSetCleanerParameters = new Gson().fromJson(parameters.get(PluginParameterKeys.DATA_SET_CLEANING_PARAMETERS), DataSetCleanerParameters.class);
        assertFalse(dataSetCleanerParameters.isUsingAltEnv());
        assertEquals(targetIndexingEnv, dataSetCleanerParameters.getTargetIndexingEnv());
    }

    @Test
    public void shouldPassDeletedRecord() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);
        String targetIndexingEnv = "PUBLISH";
        StormTaskTuple tuple = mockStormTupleFor(targetIndexingEnv);
        tuple.setMarkedAsDeleted(true);
        mockIndexerFactoryFor(null);

        indexingBolt.execute(anchorTuple, tuple);

        verifyNoInteractions(indexerPool);
        Mockito.verify(outputCollector).emit(any(Tuple.class), captor.capture());
        verify(indexerPool, never()).index(Mockito.anyString(), Mockito.any());
        verify(harvestedRecordsDAO, never()).findRecord(anyString(), anyString());
        verify(harvestedRecordsDAO, never()).updatePublishingDate(anyString(), anyString(), any(Date.class));
        Values capturedValues = captor.getValue();
        assertEquals(8, capturedValues.size());
        assertEquals("https://test.ecloud.psnc.pl/api/records/ZWUNIWERLFGQJUBIDPKLMSTHIDJMXC7U7LE6INQ2IZ32WHCZLHLA/representations/metadataRecord/versions/a9c549c0-88b1-11eb-b210-fa163e8d4ae3/files/ab67baa7-665f-418b-8c31-81713b0a324b", capturedValues.get(2));
        Map<String, String> parameters = (Map<String, String>) capturedValues.get(4);
        assertEquals(7, parameters.size());
        DataSetCleanerParameters dataSetCleanerParameters = new Gson().fromJson(parameters.get(PluginParameterKeys.DATA_SET_CLEANING_PARAMETERS), DataSetCleanerParameters.class);
        assertFalse(dataSetCleanerParameters.isUsingAltEnv());
        assertEquals(targetIndexingEnv, dataSetCleanerParameters.getTargetIndexingEnv());
    }

    @Test
    public void shouldEmitErrorNotificationForIndexerConfiguration() throws IndexingException {
        //given
        Tuple anchorTuple = mock(TupleImpl.class);
        StormTaskTuple tuple = mockStormTupleFor("PREVIEW");
        mockIndexerFactoryFor(IndexerRelatedIndexingException.class);
        //when
        indexingBolt.execute(anchorTuple, tuple);
        //then
        Mockito.verify(outputCollector).emit(any(String.class), any(Tuple.class), captor.capture());
        verify(harvestedRecordsDAO, never()).findRecord(anyString(), anyString());
        verify(harvestedRecordsDAO, never()).updatePublishingDate(anyString(), anyString(), any(Date.class));
        Values capturedValues = captor.getValue();
        Map val = (Map) capturedValues.get(2);

        assertEquals("https://test.ecloud.psnc.pl/api/records/ZWUNIWERLFGQJUBIDPKLMSTHIDJMXC7U7LE6INQ2IZ32WHCZLHLA/representations/metadataRecord/versions/a9c549c0-88b1-11eb-b210-fa163e8d4ae3/files/ab67baa7-665f-418b-8c31-81713b0a324b", val.get("resource"));
        Assert.assertTrue(val.get("additionalInfo").toString().contains("Error while indexing"));
    }

    @Test
    public void shouldEmitErrorNotificationForIndexing() throws IndexingException {
        //given
        Tuple anchorTuple = mock(TupleImpl.class);
        StormTaskTuple tuple = mockStormTupleFor("PUBLISH");
        mockIndexerFactoryFor(IndexerRelatedIndexingException.class);
        //when
        indexingBolt.execute(anchorTuple, tuple);
        //then
        verify(outputCollector).emit(any(String.class), any(Tuple.class), captor.capture());
        verify(harvestedRecordsDAO, never()).findRecord(anyString(), anyString());
        verify(harvestedRecordsDAO, never()).updatePublishingDate(anyString(), anyString(), any(Date.class));
        Values capturedValues = captor.getValue();
        Map val = (Map) capturedValues.get(2);

        assertEquals("https://test.ecloud.psnc.pl/api/records/ZWUNIWERLFGQJUBIDPKLMSTHIDJMXC7U7LE6INQ2IZ32WHCZLHLA/representations/metadataRecord/versions/a9c549c0-88b1-11eb-b210-fa163e8d4ae3/files/ab67baa7-665f-418b-8c31-81713b0a324b", val.get("resource"));
        Assert.assertTrue(val.get("additionalInfo").toString().contains("Error while indexing"));

    }

    @Test
    public void shouldThrowExceptionWhenDateIsUnParsable() throws IndexingException {
        //given
        Tuple anchorTuple = mock(TupleImpl.class);
        StormTaskTuple tuple = mockStormTupleFor("PREVIEW");
        tuple.getParameters().remove(PluginParameterKeys.METIS_RECORD_DATE);
        tuple.addParameter(PluginParameterKeys.METIS_RECORD_DATE, "UN_PARSABLE_DATE");
        //when
        indexingBolt.execute(anchorTuple, tuple);

        verify(outputCollector).emit(any(String.class), any(Tuple.class), captor.capture());
        verify(harvestedRecordsDAO, never()).findRecord(anyString(), anyString());
        verify(harvestedRecordsDAO, never()).updatePublishingDate(anyString(), anyString(), any(Date.class));
        Values capturedValues = captor.getValue();
        Map val = (Map) capturedValues.get(2);

        assertEquals("https://test.ecloud.psnc.pl/api/records/ZWUNIWERLFGQJUBIDPKLMSTHIDJMXC7U7LE6INQ2IZ32WHCZLHLA/representations/metadataRecord/versions/a9c549c0-88b1-11eb-b210-fa163e8d4ae3/files/ab67baa7-665f-418b-8c31-81713b0a324b", val.get("resource"));
        Assert.assertTrue(val.get("info_text").toString().contains("Could not parse RECORD_DATE parameter"));
        Assert.assertTrue(val.get("state").toString().equals("ERROR"));
    }

    @Test
    public void shouldThrowExceptionForUnknownEnv() throws IndexingException {
        //given
        Tuple anchorTuple = mock(TupleImpl.class);
        StormTaskTuple tuple = mockStormTupleFor("UNKNOWN_ENVIRONMENT");
        mockIndexerFactoryFor(RuntimeException.class);
        //when
        indexingBolt.execute(anchorTuple, tuple);

        verify(outputCollector).emit(any(String.class), any(Tuple.class), captor.capture());
        verify(harvestedRecordsDAO, never()).findRecord(anyString(), anyString());
        verify(harvestedRecordsDAO, never()).updatePublishingDate(anyString(), anyString(), any(Date.class));
        Values capturedValues = captor.getValue();
        Map val = (Map) capturedValues.get(2);

        assertEquals("https://test.ecloud.psnc.pl/api/records/ZWUNIWERLFGQJUBIDPKLMSTHIDJMXC7U7LE6INQ2IZ32WHCZLHLA/representations/metadataRecord/versions/a9c549c0-88b1-11eb-b210-fa163e8d4ae3/files/ab67baa7-665f-418b-8c31-81713b0a324b", val.get("resource"));
        Assert.assertTrue(val.get("state").toString().equals("ERROR"));
    }

    private void mockUisClient() throws CloudException {
        LocalId localId = new LocalId();
        localId.setProviderId("metis_test5");
        localId.setRecordId("localId");
        CloudId cloudId = new CloudId();
        cloudId.setLocalId(localId);
        cloudId.setId("cloudId");
        when(uisClient.getRecordId(anyString())).thenReturn(new ResultSlice<CloudId>(null, Arrays.asList(cloudId)));
    }

    private StormTaskTuple mockStormTupleFor(final String targetDatabase) {
        //
        return new StormTaskTuple(
                1,
                "taskName",
                "https://test.ecloud.psnc.pl/api/records/ZWUNIWERLFGQJUBIDPKLMSTHIDJMXC7U7LE6INQ2IZ32WHCZLHLA/representations/metadataRecord/versions/a9c549c0-88b1-11eb-b210-fa163e8d4ae3/files/ab67baa7-665f-418b-8c31-81713b0a324b",
                new byte[]{'a', 'b', 'c'},
                new HashMap<String, String>() {
                    {
                        put(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE, targetDatabase);
                        DateFormat dateFormat = new SimpleDateFormat(IndexingBolt.DATE_FORMAT, Locale.US);
                        put(PluginParameterKeys.METIS_RECORD_DATE, dateFormat.format(new Date()));
                        put(PluginParameterKeys.METIS_DATASET_ID, "exampleDS_ID");
                        put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "0");
                        put(PluginParameterKeys.OUTPUT_DATA_SETS,"https://test.ecloud.psnc.pl/api/data-providers/metis_test5/data-sets/4979eb22-3824-4f9a-b239-edad6c4b0bb9");
                    }
                }, new Revision());
    }

    private void mockIndexerFactoryFor(Class clazz) throws IndexingException {
        when(indexerPoolWrapper.getIndexerPool(Mockito.any(), Mockito.any())).thenReturn(indexerPool);
        if (clazz != null) {
            doThrow(clazz).when(indexerPool).index(Mockito.anyString(), Mockito.any());
        }
    }
}
