package eu.europeana.cloud.common.model;

import java.net.URI;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * File
 */
@XmlRootElement
public class File {

    private String fileName;

    private String mimeType;

    private String md5;

    private String date;

    private long contentLength;

    private URI contentUri;


    public File() {
    }


    public String getFileName() {
        return fileName;
    }


    public void setFileName(String fileName) {
        this.fileName = fileName;
    }


    public String getMimeType() {
        return mimeType;
    }


    public void setMimeType(String mimeType) {
        this.mimeType = mimeType;
    }


    public String getMd5() {
        return md5;
    }


    public void setMd5(String md5) {
        this.md5 = md5;
    }


    public String getDate() {
        return date;
    }


    public void setDate(String date) {
        this.date = date;
    }


    public long getContentLength() {
        return contentLength;
    }


    public void setContentLength(long contentLength) {
        this.contentLength = contentLength;
    }


    public URI getContentUri() {
        return contentUri;
    }


    public void setContentUri(URI contentUri) {
        this.contentUri = contentUri;
    }
}
