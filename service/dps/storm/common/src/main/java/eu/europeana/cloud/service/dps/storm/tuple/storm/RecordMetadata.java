package eu.europeana.cloud.service.dps.storm.tuple.storm;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.io.IOUtils;

import java.io.*;

@Setter
@Getter
@NoArgsConstructor
public class RecordMetadata implements Serializable {

    private static final int BATCH_MAX_SIZE = 1024 * 4;
    byte[] fileData;
    String recordUri;
    boolean markedAsDeleted;

    public RecordMetadata(String recordUri, byte[] fileData) {
        this.recordUri = recordUri;
        this.fileData = fileData;
    }


    public RecordMetadata(String recordUri, byte[] fileData, boolean markedAsDeleted) {
        this(recordUri, fileData);
        this.markedAsDeleted = markedAsDeleted;
    }

    public ByteArrayInputStream getFileByteDataAsStream() {
        if (fileData != null) {
            return new ByteArrayInputStream(fileData);
        } else {
            return null;
        }
    }

    public void setFileData(byte[] fileData) {
        this.fileData = fileData;
    }

    public void setFileData(InputStream is) throws IOException {
        try (ByteArrayOutputStream tempByteArrayOutputStream = new ByteArrayOutputStream()) {
            if (is != null) {
                byte[] buffer = new byte[BATCH_MAX_SIZE];
                IOUtils.copyLarge(is, tempByteArrayOutputStream, buffer);
                this.fileData = tempByteArrayOutputStream.toByteArray();
            } else {
                this.fileData = null;
            }
        } finally {
            //NOTE: is should be closed outside setFileData method or this method should named setFileDataAndClose
            if (is != null) {
                is.close();
            }
        }
    }
}
