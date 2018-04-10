package eu.europeana.cloud.service.mcs.rest.storage.selector;

import com.google.common.io.Resources;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;

import static eu.europeana.cloud.service.mcs.rest.Helper.readFully;
import static eu.europeana.cloud.service.mcs.rest.storage.selector.ContentStreamDetector.detectMediaType;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author krystian.
 */
@RunWith(JUnitParamsRunner.class)
public class ContentStreamDetectorTest {

    @Test
    @Parameters({
            "example_metadata.xml, application/xml",
            "example_jpg2000.jp2, image/jp2"
    })
    public void shouldProperlyGuessMimeType_Xml(String fileName, String expectedMimeType) throws
            IOException {
        //given
        URL resource = Resources.getResource(fileName);
        byte[] expected = Resources.toByteArray(resource);
        BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(
                resource.getFile()));

        //when
        String mimeType = detectMediaType(inputStream).toString();

        //then
        assertThat(mimeType,is(expectedMimeType));
        byte[] actual = readFully(inputStream, expected.length);
        assertThat(actual,is(expected));
    }
}