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
    private static final String JPEG_MIME_TYPE = "image/jpeg";
    private static final String JP2_MIME_TYPE = "image/jp2";
    private static final String FILE_NAME_WITH_PATH = "/.../.../test.tiff";
    private static final String[] TIFF_EXTENSIONS = {"tiff", "tif"};
    private static final String[] JP2_EXTENSIONS = {"jp2"};

    @Test
    public void testGoodExtension() {
        boolean isgoodExtension = ExtensionHelper.isGoodExtension(FILE_NAME_WITH_PATH, TIFF_EXTENSIONS);
        assertTrue(isgoodExtension);

    }

    @Test
    public void testbadExtension() {
        boolean isbadExtension = ExtensionHelper.isGoodExtension(FILE_NAME_WITH_PATH, JP2_EXTENSIONS);
        assertFalse(isbadExtension);

    }

    @Test
    public void testTiffMimeTypeExtension() throws MimeTypeException {
        String tiffExtension = ExtensionHelper.getExtension(TIFF_MIME_TYPE);
        assertEquals(".tiff", tiffExtension);
    }

    @Test
    public void testJp2MimeTypeExtension() throws MimeTypeException {
        String jp2Extension = ExtensionHelper.getExtension(JP2_MIME_TYPE);
        assertEquals(".jp2", jp2Extension);
    }

    @Test
    public void testJpegMimeTypeExtension() throws MimeTypeException {
        String jpegExtension = ExtensionHelper.getExtension(JPEG_MIME_TYPE);
        assertEquals(".jpg", jpegExtension);
    }

    @Test(expected = MimeTypeException.class)
    public void testUnRecognizedMimeType() throws MimeTypeException {
        String tiffExtension = ExtensionHelper.getExtension("unrecognized MimeType");
        assertEquals("tif", tiffExtension);
    }

    @Test(expected = MimeTypeException.class)
    public void testNullMimeType() throws MimeTypeException {
        ExtensionHelper.getExtension(null);
    }


}

