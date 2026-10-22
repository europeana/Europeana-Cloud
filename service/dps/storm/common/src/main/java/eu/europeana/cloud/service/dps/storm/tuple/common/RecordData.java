package eu.europeana.cloud.service.dps.storm.tuple.common;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.io.IOUtils;

import java.io.*;

@Setter
@Getter
@NoArgsConstructor
public class RecordData implements Serializable {
    // Serializable needed for SerializationUtils.clone(commonTaskTuple)

    private static final int BATCH_MAX_SIZE = 1024 * 4;
    byte[] fileData;
    String recordUri;
    String cloudId;
    boolean markedAsDepublished;

    public RecordData(String recordUri, byte[] fileData) {
        this.recordUri = recordUri;
        this.fileData = fileData;
    }


    public RecordData(String recordUri, byte[] fileData, boolean markedAsDepublished) {
        this(recordUri, fileData);
        this.markedAsDepublished = markedAsDepublished;
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
