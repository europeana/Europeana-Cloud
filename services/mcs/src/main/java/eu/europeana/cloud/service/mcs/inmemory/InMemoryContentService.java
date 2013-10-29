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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.ContentService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.FileContentHashMismatchException;

/**
 * InMemoryContentService
 */
@Service
public class InMemoryContentService implements ContentService {
    
    private Map<String, byte[]> content = new HashMap<>();
    
    @Autowired
    private RecordService recordService;
    
    
    @Override
    public void putContent(Representation rep, File file, InputStream data)
            throws IOException, FileAlreadyExistsException {
        try {
            recordService.addFileToRepresentation(rep.getRecordId(), rep.getSchema(), rep.getVersion(), file);
        } catch (FileAlreadyExistsException e) {
            // it is OK
        }
        DigestInputStream md5DigestInputStream = md5InputStream(data);
        byte[] fileContent = ByteStreams.toByteArray(md5DigestInputStream);
//        BaseEncoding.base16().encode(fileContent)
        String actualContentMd5Hex = BaseEncoding.base16().lowerCase().encode(
                md5DigestInputStream.getMessageDigest().digest());
        int actualContentLength = fileContent.length;
        
        if (file.getMd5() != null && !file.getMd5().equals(actualContentMd5Hex)) {
            throw new FileContentHashMismatchException(String.format(
                    "Declared content hash was: %s, actual: %s.", file.getMd5(), actualContentMd5Hex));
        }
        
        file.setContentLength(actualContentLength);
        file.setMd5(actualContentMd5Hex);
        content.put(generateKey(rep, file), fileContent);
    }
    
    
    private DigestInputStream md5InputStream(InputStream is) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            return new DigestInputStream(is, md);
        } catch (NoSuchAlgorithmException ex) {
            throw new AssertionError("Cannot get instance of MD5 but such algorithm should be provided", ex);
        }
    }
    
    
    @Override
    public void getContent(Representation rep, File file, long rangeStart, long rangeEnd,
            OutputStream os)
            throws IOException {
        byte[] data = content.get(generateKey(rep, file));
        if (data == null) {
            throw new FileNotExistsException();
        }
        if (rangeStart != -1 && rangeEnd != -1) {
            data = Arrays.copyOfRange(data, (int) rangeStart, (int) rangeEnd);
        }
        os.write(data);
    }
    
    
    @Override
    public void getContent(Representation rep, File file, OutputStream os)
            throws IOException {
        getContent(rep, file, -1, -1, os);
    }
    
    
    private String generateKey(String recordId, String repName, String version, String fileName) {
        return recordId + "|" + repName + "|" + version + "|" + fileName;
    }
    
    
    private String generateKey(Representation r, File file) {
        return generateKey(r.getRecordId(), r.getSchema(), r.getVersion(), file.getFileName());
    }
    
    
    @Override
    public void deleteContent(String globalId, String representationName, String version, String fileName)
            throws FileNotExistsException {
        recordService.removeFileFromRepresentation(globalId, representationName, version, fileName);
        content.remove(generateKey(globalId, representationName, version, fileName));
    }
}
