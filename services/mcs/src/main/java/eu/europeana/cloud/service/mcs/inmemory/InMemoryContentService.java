package eu.europeana.cloud.service.mcs.inmemory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.ContentService;
import eu.europeana.cloud.service.mcs.RecordService;

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
        content.put(generateKey(rep, file), consume(data));
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


    private String generateKey(Representation r, String fileName) {
        return r.getRecordId() + "|" + r.getSchema() + "|" + r.getVersion() + "|" + fileName;
    }


    private String generateKey(Representation r, File file) {
        return generateKey(r, file.getFileName());
    }


    private byte[] consume(InputStream is)
            throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int nRead;
        byte[] data = new byte[16384];

        while ((nRead = is.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }

        buffer.flush();

        return buffer.toByteArray();
    }


    @Override
    public void deleteContent(String globalId, String representationName, String version, String fileName)
            throws FileNotExistsException {
        recordService.removeFileFromRepresentation(globalId, representationName, version, fileName);
        Representation rep = new Representation();
        rep.setRecordId(globalId);
        rep.setSchema(representationName);
        rep.setVersion(version);
        content.remove(generateKey(rep, fileName));
    }
}
