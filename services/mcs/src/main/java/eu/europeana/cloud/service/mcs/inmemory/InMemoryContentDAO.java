package eu.europeana.cloud.service.mcs.inmemory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.springframework.stereotype.Repository;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileContentHashMismatchException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;

/**
 * InMemoryContentDAO
 */
@Repository
public class InMemoryContentDAO {

    private Map<String, byte[]> content = new HashMap<>();


    public void putContent(String globalId, String representationName, String version, File file, InputStream data)
            throws IOException, FileAlreadyExistsException {
        DigestInputStream md5DigestInputStream = md5InputStream(data);
        byte[] fileContent = ByteStreams.toByteArray(md5DigestInputStream);
        String actualContentMd5Hex = BaseEncoding.base16().lowerCase().encode(
                md5DigestInputStream.getMessageDigest().digest());
        int actualContentLength = fileContent.length;

        if (file.getMd5() != null && !file.getMd5().equals(actualContentMd5Hex)) {
            throw new FileContentHashMismatchException(String.format(
                    "Declared content hash was: %s, actual: %s.", file.getMd5(), actualContentMd5Hex));
        }

        file.setContentLength(actualContentLength);
        file.setMd5(actualContentMd5Hex);
        content.put(generateKey(globalId, representationName, version, file.getFileName()), fileContent);
    }


    private DigestInputStream md5InputStream(InputStream is) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            return new DigestInputStream(is, md);
        } catch (NoSuchAlgorithmException ex) {
            throw new AssertionError("Cannot get instance of MD5 but such algorithm should be provided", ex);
        }
    }


    public void getContent(String globalId, String representationName, String version, String fileName, long rangeStart, long rangeEnd,
            OutputStream os)
            throws IOException {
        byte[] data = content.get(generateKey(globalId, representationName, version, fileName));
        if (data == null) {
            throw new FileNotExistsException();
        }
        if (rangeStart != -1 && rangeEnd != -1) {
            data = Arrays.copyOfRange(data, (int) rangeStart, (int) rangeEnd);
        }
        os.write(data);
    }


    private String generateKey(String recordId, String repName, String version, String fileName) {
        return recordId + "|" + repName + "|" + version + "|" + fileName;
    }


    public void deleteContent(String globalId, String representationName, String version, String fileName)
            throws FileNotExistsException {
        content.remove(generateKey(globalId, representationName, version, fileName));
    }
}
