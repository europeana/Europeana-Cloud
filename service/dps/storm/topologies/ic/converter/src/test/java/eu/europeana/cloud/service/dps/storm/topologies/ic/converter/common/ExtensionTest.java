package eu.europeana.cloud.service.dps.storm.topologies.ic.converter.common;


import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * Created by Tarek on 9/3/2015.
 */
public class ExtensionTest {
    private String[] tiffExtensions;
    private String[] jp2Extensions;
    private String[] jpegExtensions;
    private static final String[] EXPECTED_TIFF_EXTENSIONS = {"tiff", "tif"};
    private static final String[] EXPECTED_JP2_EXTENSIONS = {"jp2"};
    private static final String[] EXPECTED_JPEG_EXTENSIONS = {"jpg", "jpeg"};

    @Before
    public void prepare() {
        tiffExtensions = Extension.Tiff.getValues();
        jp2Extensions = Extension.Jp2.getValues();
        jpegExtensions = Extension.JPEG.getValues();

    }

    @Test
    public void testTiffExtensions() {
        assertArrayEquals(tiffExtensions, EXPECTED_TIFF_EXTENSIONS);

    }

    @Test
    public void testJp2Extensions() {
        assertArrayEquals(jp2Extensions, EXPECTED_JP2_EXTENSIONS);

    }


    @Test
    public void testJpegExtensions() {
        assertArrayEquals(jpegExtensions, EXPECTED_JPEG_EXTENSIONS);

    }


}


