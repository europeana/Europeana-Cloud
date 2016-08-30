package eu.europeana.cloud.service.dps.storm.topologies.ic.converter.utils;

import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.utlis.ImageMagicHelper;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by Tarek on 18/8/2016.
 */
public class ImageMagicHelperTest {
    private ImageMagicHelper imageMagicHelper;
    private static final String INPUT_FILE = "path/inputFileName.jpg";
    private static final String OUTPUT_FILE = "path/outputFileName.tiff";
    private static final String IMAGE_MAGIC_CONVERT = "magick";
    private static final String IMAGE_MAGIC_CONVERT_CONSOLE_COMMAND_WITHOUT_PROPERTIES = "magick path/inputFileName.jpg path/outputFileName.tiff";
    private static final String IMAGE_MAGIC_CONVERT_CONSOLE_COMMAND_WITH_PROPERTIES = "magick path/inputFileName.jpg path/outputFileName.tiff -flush_period 1024 Clayers=28 -rate  0.0000001";

    @Before
    public void prepare() {
        imageMagicHelper = new ImageMagicHelper();
    }

    @Test
    public void testCompressCommandWithoutProperties() {
        String command = imageMagicHelper.constructCommand(IMAGE_MAGIC_CONVERT, INPUT_FILE, OUTPUT_FILE, null);
        assertEquals(command, IMAGE_MAGIC_CONVERT_CONSOLE_COMMAND_WITHOUT_PROPERTIES);
    }

    @Test
    public void testCompressCommandWithProperties() {
        List<String> properties = buildProperties();
        String command = imageMagicHelper.constructCommand(IMAGE_MAGIC_CONVERT, INPUT_FILE, OUTPUT_FILE, properties);
        assertEquals(command, IMAGE_MAGIC_CONVERT_CONSOLE_COMMAND_WITH_PROPERTIES);
    }

    @Test
    public void testCompressCommandWithNullInputFile() {
        String command = imageMagicHelper.constructCommand(IMAGE_MAGIC_CONVERT, null, OUTPUT_FILE, buildProperties());
        assertNull(command);

    }

    @Test
    public void testCompressCommandWithNullOutputFile() {
        String command = imageMagicHelper.constructCommand(IMAGE_MAGIC_CONVERT, INPUT_FILE, null, buildProperties());
        assertNull(command);

    }

    @Test
    public void testCompressCommandWithNullFiles() {
        String command = imageMagicHelper.constructCommand(IMAGE_MAGIC_CONVERT, null, null, buildProperties());
        assertNull(command);

    }

    private List<String> buildProperties() {
        List<String> properties = new ArrayList<String>();
        properties.add("-flush_period 1024");
        properties.add("Clayers=28");
        properties.add("-rate  0.0000001");
        return properties;
    }

}