package eu.europeana.cloud.common.model;

import java.net.URI;
import java.util.ArrayList;
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

    private URI uri;

    private String dataProvider;

    private List<File> files = new ArrayList<File>(0);

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


    public URI getUri() {
        return uri;
    }


    public void setUri(URI selfUri) {
        this.uri = selfUri;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((allVersionsUri == null) ? 0 : allVersionsUri.hashCode());
        result = prime * result
                + ((dataProvider == null) ? 0 : dataProvider.hashCode());
        result = prime * result + ((files == null) ? 0 : files.hashCode());
        result = prime * result + (persistent ? 1231 : 1237);
        result = prime * result
                + ((recordId == null) ? 0 : recordId.hashCode());
        result = prime * result + ((schema == null) ? 0 : schema.hashCode());
        result = prime * result + ((uri == null) ? 0 : uri.hashCode());
        result = prime * result + ((version == null) ? 0 : version.hashCode());
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Representation other = (Representation) obj;
        if (allVersionsUri == null) {
            if (other.allVersionsUri != null) {
                return false;
            }
        } else if (!allVersionsUri.equals(other.allVersionsUri)) {
            return false;
        }
        if (dataProvider == null) {
            if (other.dataProvider != null) {
                return false;
            }
        } else if (!dataProvider.equals(other.dataProvider)) {
            return false;
        }
        if (files == null) {
            if (other.files != null) {
                return false;
            }
        } else if (!files.equals(other.files)) {
            return false;
        }
        if (persistent != other.persistent) {
            return false;
        }
        if (recordId == null) {
            if (other.recordId != null) {
                return false;
            }
        } else if (!recordId.equals(other.recordId)) {
            return false;
        }
        if (schema == null) {
            if (other.schema != null) {
                return false;
            }
        } else if (!schema.equals(other.schema)) {
            return false;
        }
        if (uri == null) {
            if (other.uri != null) {
                return false;
            }
        } else if (!uri.equals(other.uri)) {
            return false;
        }
        if (version == null) {
            if (other.version != null) {
                return false;
            }
        } else if (!version.equals(other.version)) {
            return false;
        }
        return true;
    }


    @Override
    public String toString() {
        return "Representation [recordId=" + recordId + ", schema=" + schema
                + ", version=" + version + ", allVersionsUri=" + allVersionsUri
                + ", selfUri=" + uri + ", dataProvider=" + dataProvider
                + ", files=" + files + ", persistent=" + persistent + "]";
    }
}
