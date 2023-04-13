package eu.europeana.cloud.service.mcs.utils.storage_selector;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.io.Resources;
import eu.europeana.cloud.service.mcs.controller.Helper;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.commons.io.input.NullInputStream;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * @author krystian.
 */
@RunWith(JUnitParamsRunner.class)
public class PreBufferedInputStreamTest {

  @Test
  @Parameters({
      "10, 20",
      "20, 10",
      "10, 10000",
      "10000, 20",
      "10000, 3000",
      "10000, 4000",
      "10, 20",
      "20, 10"
  })
  public void shouldProperlyCheckAvailableForByteArrayInputStream(int size, int bufferSize) throws
      IOException {
    //given
    byte[] expected = Helper.generateRandom(size);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(expected);
    PreBufferedInputStream instance = new PreBufferedInputStream(inputStream, bufferSize);

    //when
    int available = instance.available();

    //then
    assertThat(available, is(size));
    assertUnchangedStream(expected, instance);
  }

  @Test
  @Parameters({
      "10, example_metadata.xml",
      "200, example_metadata.xml",
      "2000, example_metadata.xml",
      "3000, example_metadata.xml",
      "10, example_jpg2000.jp2",
      "2000, example_jpg2000.jp2",
      "3000, example_jpg2000.jp2"
  })
  public void shouldProperlyCheckAvailableForFile(final int bufferSize,
      final String fileName) throws IOException {
    //given
    URL resourceUri = Resources.getResource(fileName);
    final byte[] expected = Resources.toByteArray(resourceUri);
    FileInputStream inputStream = new FileInputStream(
        resourceUri.getFile());
    PreBufferedInputStream instance = new PreBufferedInputStream(inputStream, bufferSize);

    //when
    int available = instance.available();

    //then
    assertThat(available, is((expected.length)));
    assertThat(available, is(((int) new File(resourceUri.getFile()).length())));
    assertUnchangedStream(expected, instance);
  }

  private void assertUnchangedStream(byte[] expected, PreBufferedInputStream instance) throws IOException {
    byte[] actual = Helper.readFully(instance, expected.length);
    assertThat(actual, is(expected));
  }

  @Test
  @Parameters({
      "10, 20",
      "200, 10",
      "100, 10000",
      "100, 200",
      "200, 100"
  })
  public void shouldReadStreamPerByte(int size, int bufferSize) throws IOException {
    //given
    byte[] randomByes = Helper.generateSeq(size);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(randomByes);
    PreBufferedInputStream instance = new PreBufferedInputStream(inputStream, bufferSize);
    int[] actual = new int[randomByes.length];

    //when
    for (int i = 0; i < actual.length; i++) {
      actual[i] = instance.read();
    }

    //then
    int[] randomIntegers = new int[actual.length];
    for (int i = 0; i < randomIntegers.length; i++) {
      randomIntegers[i] = randomByes[i] & 0xff;
    }
    assertThat(actual, is(randomIntegers));
  }

  @Test(expected = IOException.class)
  public void shouldThrowIOExceptionOnTryToOperateOnClosed() throws IOException {
    //given
    PreBufferedInputStream instance =
        new PreBufferedInputStream(new ByteArrayInputStream("".getBytes()), 10);
    instance.close();

    //then
    instance.read();
  }

  @Test
  @Parameters({
      "10, 20, 0",
      "10, 20, 5",
      "20, 10, 12",
      "10, 10000, 4",
      "10000, 20, 300",
      "10000, 3000, 5000",
      "10000, 4000, 6000",
      "10, 20, 300",
      "20, 10, 55"
  })
  public void shouldProperlySkip(int size, int bufferSize, int skip) throws IOException {
    //given
    byte[] bytes = Helper.generateRandom(size);
    byte[] expected = subArray(bytes, skip, size);

    ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
    PreBufferedInputStream instance = new PreBufferedInputStream(inputStream, bufferSize);

    //when
    long skipped = instance.skip(skip);

    //then
    long expectedSkip = Math.min(skip, size);
    assertThat(skipped, is(expectedSkip));
    assertThat(instance.available(), is(size - (int) expectedSkip));
    assertUnchangedStream(expected, instance);
  }

  @Test
  @Parameters({
      "10, 20, 5",
      "10, 10000, 4",
      "10000, 200, 100",
      "10000, 3000, 2000",
      "10000, 4000, 3000"
  })
  public void shouldProperlyMarkAndReset(int size, int bufferSize, int mark) throws IOException {
    //given
    byte[] bytes = Helper.generateRandom(size);
    byte[] expected = subArray(bytes, 0, size);

    ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
    PreBufferedInputStream instance = new PreBufferedInputStream(inputStream, bufferSize);

    //when
    instance.mark(mark);
    instance.read();
    instance.read();
    instance.reset();

    //then

    assertThat(instance.available(), is(size));
    assertUnchangedStream(expected, instance);
  }

  @Test
  public void shouldCloseClosedStream() throws IOException {
    //given
    PreBufferedInputStream instance = new PreBufferedInputStream(new NullInputStream(1), 1);
    instance.close();
    //then
    instance.close();
  }

  private byte[] subArray(byte[] bytes, int skip, int size) {
    if (size - skip <= 0) {
      return new byte[0];
    }
    byte[] sub = new byte[size - skip];
    System.arraycopy(bytes, skip, sub, 0, sub.length);
    return sub;
  }
}