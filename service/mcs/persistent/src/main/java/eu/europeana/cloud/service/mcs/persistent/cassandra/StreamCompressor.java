package eu.europeana.cloud.service.mcs.persistent.cassandra;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Compress and decompress stream data via gzip algorithm.
 *
 * @author krystian
 */
public class StreamCompressor {

    void decompress(byte[] compressedBytes, OutputStream os) throws IOException {
        ByteArrayInputStream is = new ByteArrayInputStream(compressedBytes);
        GZIPInputStream gis = new GZIPInputStream(is);
        IOUtils.copy(gis, os);
    }

    byte[] compress(final InputStream is) throws IOException {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream();
             GZIPOutputStream gos = new GZIPOutputStream(os)) {
            IOUtils.copy(is, gos);
            return os.toByteArray();
        }
    }
}