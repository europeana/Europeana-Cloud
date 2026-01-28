package eu.europeana.cloud.service.mcs.persistent.cassandra;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author krystian.
 */
public class StreamCompressorTest {

  StreamCompressor instance = new StreamCompressor();

  @Test
  void shouldCompressAndDecompressContent() throws Exception {
    //given
    byte[] bytes = "Test content".getBytes();
    ByteArrayInputStream is = new ByteArrayInputStream(bytes);
    ByteArrayOutputStream os = new ByteArrayOutputStream();

    //when
    byte[] compressedBytes = instance.compress(is);
    instance.decompress(compressedBytes, os);

    //then
    assertThat(os.toByteArray(), is(bytes));
  }

}