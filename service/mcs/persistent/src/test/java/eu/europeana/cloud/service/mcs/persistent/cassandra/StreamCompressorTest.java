package eu.europeana.cloud.service.mcs.persistent.cassandra;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Test;

import java.io.ByteArrayInputStream;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author krystian.
 */
public class StreamCompressorTest {
    StreamCompressor instance = new StreamCompressor();

    @Test
    public void shouldCompressAndDecompressContent() throws Exception {
        //given
        byte[] bytes = "Test content".getBytes();
        ByteArrayInputStream is = new ByteArrayInputStream(bytes);
        ByteArrayOutputStream os = new ByteArrayOutputStream();

        //when
        byte[] compressedBytes = instance.compress(is);
        instance.decompress(compressedBytes, os);

        //then
        assertThat(os.toByteArray(),is(bytes));
    }

}