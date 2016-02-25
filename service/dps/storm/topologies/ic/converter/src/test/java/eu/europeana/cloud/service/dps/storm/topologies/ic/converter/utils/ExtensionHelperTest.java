package eu.europeana.cloud.service.dps.storm.topologies.ic.converter.utils;

import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.utlis.ExtensionHelper;
import org.apache.tika.mime.MimeTypeException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Tarek on 9/3/2015.
 */
public class ExtensionHelperTest {
    private static final String TIFF_MIME_TYPE = "image/tiff";
    private static final String TIFF_EXTENSION = "tiff";
    private ExtensionHelper extensionHelper;
    private static final String FILE_NAME_WITH_PATH = "/.../.../test.tiff";
    private static final String[] TIFF_EXTENSIONS = {"tiff", "tif"};
    private static final String[] JP2_EXTENSIONS = {"jp2"};

    /**
     * Prepare the unit tests
     */
    @Before
    public void prepare() {
        extensionHelper = new ExtensionHelper();
    }

    @Test
    public void testGoodExtension() {
        boolean isgoodExtension = extensionHelper.isGoodExtension(FILE_NAME_WITH_PATH, TIFF_EXTENSIONS);
        assertTrue(isgoodExtension);

    }

    @Test
    public void testbadExtension() {
        boolean isbadExtension = extensionHelper.isGoodExtension(FILE_NAME_WITH_PATH, JP2_EXTENSIONS);
        assertFalse(isbadExtension);

    }

}

