package eu.europeana.cloud.normalization.bolts;

import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.normalization.Normalizer;
import eu.europeana.normalization.NormalizerFactory;
import eu.europeana.normalization.model.NormalizationReport;
import eu.europeana.normalization.model.NormalizationResult;
import eu.europeana.normalization.util.NormalizationConfigurationException;
import eu.europeana.normalization.util.NormalizationException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.util.HashMap;
import java.util.Map;

import static eu.europeana.cloud.service.dps.test.TestConstants.SOURCE_VERSION_URL;
import static org.mockito.Mockito.when;

public class NormalizationBoltTest {

    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

    @Mock
    public NormalizerFactory normalizerFactory;

    @Mock
    public Normalizer normalizer;

    @InjectMocks
    private NormalizationBolt normalizationBolt = new NormalizationBolt();

    @Before
    public void init() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(normalizerFactory.getNormalizer()).thenReturn(normalizer);
    }

    @Test
    public void shouldNormalizeRecord() throws Exception {
        //given
        String expected = "expected result";
        NormalizationResult result = NormalizationResult.createInstanceForSuccess(expected, new NormalizationReport());
        when(normalizer.normalize(Mockito.anyString())).thenReturn(result);

        //when
        normalizationBolt.execute(getCorrectStormTuple());

        //then
        Mockito.verify(outputCollector, Mockito.times(1)).emit(Mockito.any(Tuple.class), captor.capture());
        Values capturedValues = captor.getValue();
        Assert.assertArrayEquals(expected.getBytes(), (byte[]) capturedValues.get(3));
    }

    @Test
    public void shouldEmitErrorWhenNormalizationResultContainsErrorMessage() throws Exception {
        //given
        String expected = "expected result";
        NormalizationResult result = NormalizationResult.createInstanceForError("some error message from normalization plugin", expected);
        when(normalizer.normalize(Mockito.anyString())).thenReturn(result);

        //when
        normalizationBolt.execute(getCorrectStormTuple());

        //then
        Mockito.verify(outputCollector, Mockito.times(1)).emit(Mockito.anyString(), Mockito.any(Tuple.class), captor.capture());
        Values capturedValues = captor.getValue();
        Map val = (Map) capturedValues.get(2);
        Assert.assertEquals("Error during normalization.", val.get("additionalInfo"));
        Assert.assertEquals("some error message from normalization plugin", val.get("info_text"));
    }


    @Test
    public void shouldEmitErrorWhenNormalizationConfigurationExceptionThrownFromPlugIn() throws Exception {
        //given
        when(normalizer.normalize(Mockito.anyString())).thenThrow(NormalizationConfigurationException.class);

        //when
        normalizationBolt.execute(getCorrectStormTuple());

        //then
        Mockito.verify(outputCollector, Mockito.times(1)).emit(Mockito.anyString(), Mockito.any(Tuple.class), captor.capture());
        Values capturedValues = captor.getValue();
        Map val = (Map) capturedValues.get(2);
        Assert.assertEquals("Error in normalizer configuration", val.get("additionalInfo"));
    }

    @Test
    public void shouldEmitErrorWhenNormalizationExceptionThrownFromPlugIn() throws Exception {
        //given
        when(normalizer.normalize(Mockito.anyString())).thenThrow(NormalizationException.class);

        //when
        normalizationBolt.execute(getCorrectStormTuple());

        //then
        Mockito.verify(outputCollector, Mockito.times(1)).emit(Mockito.anyString(), Mockito.any(Tuple.class), captor.capture());
        Values capturedValues = captor.getValue();
        Map val = (Map) capturedValues.get(2);
        Assert.assertEquals("Error during normalization.", val.get("additionalInfo"));
    }

    @Test
    public void shouldEmitErrorWhenCantPrepareTupleForEmission() throws Exception {
        //given
        String expected = "expected result";
        NormalizationResult result = NormalizationResult.createInstanceForSuccess(expected, new NormalizationReport());
        when(normalizer.normalize(Mockito.anyString())).thenReturn(result);

        //when
        normalizationBolt.execute(getMalformedStormTuple());

        //then
        Mockito.verify(outputCollector, Mockito.times(1)).emit(Mockito.anyString(), Mockito.any(Tuple.class), captor.capture());
        Values capturedValues = captor.getValue();
        Map val = (Map) capturedValues.get(2);
        Assert.assertEquals("Cannot prepare output storm tuple.", val.get("additionalInfo"));
    }


    private StormTaskTuple getCorrectStormTuple(){
        return getStormTuple(SOURCE_VERSION_URL);
    }

    private StormTaskTuple getMalformedStormTuple(){
        return getStormTuple("malformed.url");
    }

    private StormTaskTuple getStormTuple(String fileUrl) {
        byte[] FILE_DATA = new byte[]{'a', 'b', 'c'};
        StormTaskTuple tuple = new StormTaskTuple(123, "TASK_NAME", fileUrl, FILE_DATA, new HashMap<String, String>(), null);
        return tuple;
    }
}