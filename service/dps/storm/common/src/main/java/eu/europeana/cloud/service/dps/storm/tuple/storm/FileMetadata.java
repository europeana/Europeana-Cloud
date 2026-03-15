package eu.europeana.cloud.service.dps.storm.tuple.storm;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

@Setter
@Getter
public class FileMetadata {

    private static final int BATCH_MAX_SIZE = 1024 * 4;
    String fileUrl;
    byte[] fileData;

    public FileMetadata(String fileUrl, byte[] fileData) {
        this.fileUrl = fileUrl;
        this.fileData = fileData;
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
