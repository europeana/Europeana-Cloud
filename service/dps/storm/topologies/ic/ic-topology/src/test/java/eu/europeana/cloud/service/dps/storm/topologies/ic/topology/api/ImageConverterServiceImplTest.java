package eu.europeana.cloud.service.dps.storm.topologies.ic.topology.api;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.converter.ConverterContext;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.converter.KakaduConverterTiffToJP2;
import eu.europeana.cloud.service.dps.storm.topologies.ic.topology.util.ImageConverterUtil;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import static eu.europeana.cloud.service.dps.test.TestConstants.*;

import static org.junit.Assert.*;

import java.io.InputStream;

import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.*;


public class ImageConverterServiceImplTest {

    private ImageConverterServiceImpl imageConverterService;
    private ConverterContext converterContext;
    private ImageConverterUtil imageConverterUtil;
    InputStream sourceStream;
    InputStream convertedStream;
    private final static String JP2_EXTENSION = "jp2";
    private final static String AUTHORIZATION_HEADER = "AUTHORIZATION_HEADER";


    @Before
    public void init() {
        sourceStream = IOUtils.toInputStream("stream of the source image");
        convertedStream = IOUtils.toInputStream("stream of the converted image");
        converterContext = mock(ConverterContext.class);
        imageConverterUtil = mock(ImageConverterUtil.class);
        imageConverterService = new ImageConverterServiceImpl(converterContext, imageConverterUtil);


    }

    @Test
    public void testConvertTiffToJp2() throws Exception {
        doNothing().when(converterContext).setConverter(new KakaduConverterTiffToJP2());
        doNothing().when(converterContext).convert(anyString(), anyString(), anyList());
        StormTaskTuple stormTaskTuple = prepareStormTaskTuple("image/tiff");
        when(imageConverterUtil.getStream(anyString())).thenReturn(convertedStream);
        imageConverterService.convertFile(stormTaskTuple);
        assertResultedStormTaskTuple(stormTaskTuple);
    }

    @Test(expected = AssertionError.class)
    public void testUnrecognizedMimeType() throws Exception {
        doNothing().when(converterContext).setConverter(new KakaduConverterTiffToJP2());
        doNothing().when(converterContext).convert(anyString(), anyString(), anyList());
        StormTaskTuple stormTaskTuple = prepareStormTaskTuple("Undefined");
        when(imageConverterUtil.getStream(anyString())).thenReturn(convertedStream);
        imageConverterService.convertFile(stormTaskTuple);

    }

    @Test
    public void testConvertJpegToJp2() throws Exception {
        doNothing().when(converterContext).setConverter(new KakaduConverterTiffToJP2());
        doNothing().when(converterContext).convert(anyString(), anyString(), anyList());
        StormTaskTuple stormTaskTuple = prepareStormTaskTuple("image/jpeg");
        when(imageConverterUtil.getStream(anyString())).thenReturn(convertedStream);
        imageConverterService.convertFile(stormTaskTuple);
        assertResultedStormTaskTuple(stormTaskTuple);
    }


    private StormTaskTuple prepareStormTaskTuple(String mimeType) throws Exception {
        StormTaskTuple stormTaskTuple = new StormTaskTuple();
        stormTaskTuple.addParameter(PluginParameterKeys.MIME_TYPE, mimeType);
        stormTaskTuple.addParameter(PluginParameterKeys.OUTPUT_MIME_TYPE, "image/jp2");
        stormTaskTuple.addParameter(PluginParameterKeys.AUTHORIZATION_HEADER, AUTHORIZATION_HEADER);
        stormTaskTuple.setFileUrl(SOURCE_VERSION_URL);
        stormTaskTuple.setFileData(sourceStream);
        return stormTaskTuple;

    }

    private void assertResultedStormTaskTuple(StormTaskTuple stormTaskTuple) {
        assertNotNull(stormTaskTuple.getParameters());
        assertEquals(stormTaskTuple.getParameters().size(), 6);

        String outputFileName = stormTaskTuple.getParameter(PluginParameterKeys.OUTPUT_FILE_NAME);
        assertNotNull(outputFileName);
        assertEquals(FilenameUtils.getExtension(outputFileName), JP2_EXTENSION);

        String cloudId = stormTaskTuple.getParameter(PluginParameterKeys.CLOUD_ID);
        assertNotNull(cloudId);
        assertEquals(cloudId, SOURCE + CLOUD_ID);

        String representationName = stormTaskTuple.getParameter(PluginParameterKeys.REPRESENTATION_NAME);
        assertNotNull(representationName);
        assertEquals(representationName, SOURCE + REPRESENTATION_NAME);

        String version = stormTaskTuple.getParameter(PluginParameterKeys.REPRESENTATION_VERSION);
        assertNotNull(version);
        assertEquals(version, SOURCE + VERSION);

        String outputMimeType = stormTaskTuple.getParameter(PluginParameterKeys.OUTPUT_MIME_TYPE);
        assertNotNull(outputMimeType);
        assertEquals(outputMimeType, "image/jp2");

        String authorizationHeader = stormTaskTuple.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER);
        assertNotNull(authorizationHeader);
        assertEquals(authorizationHeader, AUTHORIZATION_HEADER);

        assertNotNull(stormTaskTuple.getFileByteDataAsStream());
        assertNotNull(stormTaskTuple.getFileData());

    }


}