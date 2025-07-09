package eu.europeana.cloud.service.mcs.utils.storage_selector;

import static eu.europeana.cloud.service.mcs.controller.Helper.readFully;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.io.Resources;
import eu.europeana.cloud.service.mcs.Storage;
import jakarta.ws.rs.BadRequestException;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * @author krystian.
 */
@RunWith(JUnitParamsRunner.class)
public class StorageSelectorTest {

  @Test
  @Parameters({
      "example_metadata.xml, DB_STORAGE, application/xml",
      "example_jpg2000.jp2, OBJECT_STORAGE, image/jp2"
  })
  public void shouldDetectStorage(String fileName, String expectedDecision, String mediaType) throws
      IOException {
    //given
    URL resource = Resources.getResource(fileName);
    byte[] expected = Resources.toByteArray(resource);
    PreBufferedInputStream inputStream = new PreBufferedInputStream(
        new FileInputStream(resource.getFile()), 512 * 1024);

    StorageSelector instance = new StorageSelector(inputStream, mediaType);

    //when
    Storage decision = instance.selectStorage();

    //then
    assertThat(decision.toString(), is(expectedDecision));
    byte[] actual = readFully(inputStream, expected.length);
    assertThat(actual, is(expected));
  }

  @Test(expected = BadRequestException.class)
  @Parameters({
      "example_metadata.xml, image/jp2",
      "example_jpg2000.jp2, application/xml"
  })
  public void shouldThrowBadRequestOnDifferentMimeTypeProvidedByUser(String fileName,
      String mediaType)
      throws IOException {
    //given
    URL resource = Resources.getResource(fileName);
    byte[] expected = Resources.toByteArray(resource);
    PreBufferedInputStream inputStream = new PreBufferedInputStream(
        new FileInputStream(resource.getFile()), 512 * 1024);

    StorageSelector instance = new StorageSelector(inputStream, mediaType);

    //then
    Storage decision = instance.selectStorage();


  }


}