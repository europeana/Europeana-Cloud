package eu.europeana.cloud.service.dps.storm.topologies.ic.converter.utils;

/**
 * Created by Tarek on 9/3/2015.
 */

import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.exceptions.UnexpectedExtensionsException;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.utlis.MimeTypeHelper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class MimeTypeHelperTest {
    private static final String TIFF_MIME_TYPE = "image/tiff";
     private static final String TIFF_EXTENSION = "tiff";

    @Test
    public void testRecognizedExtension() throws UnexpectedExtensionsException {
        String mimeType = MimeTypeHelper.getMimeTypeFromExtension(TIFF_EXTENSION);
        assertEquals(mimeType, TIFF_MIME_TYPE);

    }

    @Test(expected = UnexpectedExtensionsException.class)
    public void testUnRecognizedExtension() throws UnexpectedExtensionsException {
        MimeTypeHelper.getMimeTypeFromExtension("UndefinedExtension");
    }

}

