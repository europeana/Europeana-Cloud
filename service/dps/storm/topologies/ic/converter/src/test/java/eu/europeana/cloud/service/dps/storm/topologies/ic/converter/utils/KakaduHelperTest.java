package eu.europeana.cloud.service.dps.storm.topologies.ic.converter.utils;

import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.utlis.KakaduHelper;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by Tarek on 9/3/2015.
 */
public class KakaduHelperTest {
    private KakaduHelper kakaduHelper;
    private static final String INPUT_FILE = "path/inputFileName.ext";
    private static final String OUTPUT_FILE = "path/outputFileName.ext";
    private static final String KAKADU_COMPRESS = "kdu_compress";
    private static final String KAKADU_COMPRESS_CONSOLE_COMMAND_WITHOUT_PROPERTIES = "kdu_compress -i path/inputFileName.ext -o path/outputFileName.ext";
    private static final String KAKADU_COMPRESS_CONSOLE_COMMAND_WITH_PROPERTIES = "kdu_compress -i path/inputFileName.ext -o path/outputFileName.ext -flush_period 1024 Clayers=28 -rate  0.0000001";

    @Before
    public void prepare() {
        kakaduHelper = new KakaduHelper();
    }

    @Test
    public void testCompressCommandWithoutProperties() {
        String compressCommand = kakaduHelper.constructCommand(KAKADU_COMPRESS, INPUT_FILE, OUTPUT_FILE, null);
        assertEquals(compressCommand, KAKADU_COMPRESS_CONSOLE_COMMAND_WITHOUT_PROPERTIES);
    }

    @Test
    public void testCompressCommandWithProperties() {
        List<String> properties = buildProperties();
        String compressCommand = kakaduHelper.constructCommand(KAKADU_COMPRESS, INPUT_FILE, OUTPUT_FILE, properties);
        assertEquals(compressCommand, KAKADU_COMPRESS_CONSOLE_COMMAND_WITH_PROPERTIES);
    }

    @Test
    public void testCompressCommandWithNullInputFile() {
        String compressCommand = kakaduHelper.constructCommand(KAKADU_COMPRESS, null, OUTPUT_FILE, buildProperties());
        assertNull(compressCommand);

    }

    @Test
    public void testCompressCommandWithNullOutputFile() {
        String compressCommand = kakaduHelper.constructCommand(KAKADU_COMPRESS, INPUT_FILE, null, buildProperties());
        assertNull(compressCommand);

    }

    @Test
    public void testCompressCommandWithNullFiles() {
        String compressCommand = kakaduHelper.constructCommand(KAKADU_COMPRESS, null, null, buildProperties());
        assertNull(compressCommand);

    }

    private List<String> buildProperties() {
        List<String> properties = new ArrayList<String>();
        properties.add("-flush_period 1024");
        properties.add("Clayers=28");
        properties.add("-rate  0.0000001");
        return properties;
    }

}