package eu.europeana.cloud.contentserviceapi.model;

import java.net.URI;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * Representation
 */
@XmlRootElement
public class Representation {

    private String recordId;

    private String schema;

    private String version;

    private URI allVersionsUri;

    private URI selfUri;

    private String dataProvider;

    private List<File> files;

    private boolean persistent;


    public String getRecordId() {
        return recordId;
    }


    public void setRecordId(String recordId) {
        this.recordId = recordId;
    }


    public String getSchema() {
        return schema;
    }


    public void setSchema(String schema) {
        this.schema = schema;
    }


    public String getVersion() {
        return version;
    }


    public void setVersion(String version) {
        this.version = version;
    }


    public String getDataProvider() {
        return dataProvider;
    }


    public void setDataProvider(String dataProvider) {
        this.dataProvider = dataProvider;
    }


    public List<File> getFiles() {
        return files;
    }


    public void setFiles(List<File> files) {
        this.files = files;
    }


    public boolean isPersistent() {
        return persistent;
    }


    public void setPersistent(boolean persistent) {
        this.persistent = persistent;
    }


    public URI getAllVersionsUri() {
        return allVersionsUri;
    }


    public void setAllVersionsUri(URI allVersionsUri) {
        this.allVersionsUri = allVersionsUri;
    }


    public URI getSelfUri() {
        return selfUri;
    }


    public void setSelfUri(URI selfUri) {
        this.selfUri = selfUri;
    }
}
