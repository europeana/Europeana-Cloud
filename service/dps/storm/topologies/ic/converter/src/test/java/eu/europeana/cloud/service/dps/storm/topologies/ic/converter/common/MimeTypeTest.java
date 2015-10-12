package eu.europeana.cloud.service.dps.storm.topologies.ic.converter.common;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by Tarek on 9/3/2015.
 */
public class MimeTypeTest {
    private String tiffMimeType;
    private String jp2MimeType;
    private static final String EXPECTED_TIFF_MIME_TYPE = "image/tiff";
    private static final String EXPECTED_JP2_MIME_TYPE = "image/jp2";

    @Before
    public void prepare() {
        tiffMimeType = MimeType.MIME_IMAGE_TIFF.getValue();
        jp2MimeType = MimeType.MIME_IMAGE_JP2.getValue();

    }

    @Test
    public void testTiffExtensions() {
        assertEquals(tiffMimeType,EXPECTED_TIFF_MIME_TYPE);

    }

    @Test
    public void testjp2Extensions() {
        assertEquals(jp2MimeType, EXPECTED_JP2_MIME_TYPE);

    }


}


