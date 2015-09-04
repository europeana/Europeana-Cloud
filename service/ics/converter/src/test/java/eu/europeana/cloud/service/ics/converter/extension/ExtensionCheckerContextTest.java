package eu.europeana.cloud.service.ics.converter.extension;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by Tarek on 9/3/2015.
 */
public class ExtensionCheckerContextTest {
    private ExtensionCheckerContext extensionCheckerContext;
    private static final String TIFF_FILE_WITH_PATH = "/.../.../test.tiff";
    private static final String JP2_FILE_WITH_PATH = "/.../.../test.jp2";

    @Test
    public void testTiffExtensionWithRightContext() {
        extensionCheckerContext = new ExtensionCheckerContext(new TiffExtensionChecker());
        assertTrue(extensionCheckerContext.isGoodExtension(TIFF_FILE_WITH_PATH));

    }

    @Test
    public void testJp2ExtensionWithRightContext() {
        extensionCheckerContext = new ExtensionCheckerContext(new JP2ExtensionChecker());
        assertTrue(extensionCheckerContext.isGoodExtension(JP2_FILE_WITH_PATH));

    }

    @Test
    public void testTiffExtensionWithWrongContext() {
        extensionCheckerContext = new ExtensionCheckerContext(new JP2ExtensionChecker());
        assertFalse(extensionCheckerContext.isGoodExtension(TIFF_FILE_WITH_PATH));

    }


}
