package eu.europeana.cloud.service.mcs.inmemory;

import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
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

/**
 * InMemoryContentDAO
 */
@Repository
public class InMemoryContentDAO {

	private final Map<String, byte[]> content = new HashMap<>();


    public void putContent(String globalId, String schema, String version, File file, InputStream data)
            throws IOException {
        DigestInputStream md5DigestInputStream = md5InputStream(data);
        byte[] fileContent = ByteStreams.toByteArray(md5DigestInputStream);
        String actualContentMd5Hex = BaseEncoding.base16().lowerCase().encode(
                md5DigestInputStream.getMessageDigest().digest());
        int actualContentLength = fileContent.length;

//        if (file.getMd5() != null && !file.getMd5().equals(actualContentMd5Hex)) {
//            throw new FileContentHashMismatchException(String.format(
//                    "Declared content hash was: %s, actual: %s.", file.getMd5(), actualContentMd5Hex));
//        }

        file.setContentLength(actualContentLength);
        file.setMd5(actualContentMd5Hex);
        content.put(generateKey(globalId, schema, version, file.getFileName()), fileContent);
    }


    private DigestInputStream md5InputStream(InputStream is) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            return new DigestInputStream(is, md);
        } catch (NoSuchAlgorithmException ex) {
            throw new AssertionError("Cannot get instance of MD5 but such algorithm should be provided", ex);
        }
    }


    public void getContent(String globalId, String schema, String version, String fileName, long rangeStart, long rangeEnd,
            OutputStream os)
            throws IOException, FileNotExistsException, WrongContentRangeException {
        byte[] data = content.get(generateKey(globalId, schema, version, fileName));
        if (data == null) {
            throw new FileNotExistsException();
        }
        if (rangeStart != -1) {
            if (rangeStart > data.length - 1) {
                throw new WrongContentRangeException("Cannot satisfy requested range - data length is " + data.length);
            }
            if (rangeEnd == -1) {
                rangeEnd = data.length - 1;
            }
            if (rangeEnd > data.length - 1){
                rangeEnd = data.length - 1;
            }
            data = Arrays.copyOfRange(data, (int) rangeStart, (int) rangeEnd + 1);
        }
        os.write(data);
    }


    public void copyContent(String srcGlobalId, String srcRepName, String srcVersion, String srcFileName,
            String trgGlobalId, String trgRepName, String trgVersion, String trgFileName) throws FileNotExistsException {
        String srcKey = generateKey(srcGlobalId, srcRepName, srcVersion, srcFileName);
        String trgKey = generateKey(trgGlobalId, trgRepName, trgVersion, trgFileName);
        byte[] data = content.get(srcKey);
        if (data == null) {
            throw new FileNotExistsException();
        }
        content.put(trgKey, data);
    }


    private String generateKey(String recordId, String repName, String version, String fileName) {
        return recordId + "|" + repName + "|" + version + "|" + fileName;
    }


    public void deleteContent(String globalId, String schema, String version, String fileName)
            throws FileNotExistsException {
        content.remove(generateKey(globalId, schema, version, fileName));
    }
}
