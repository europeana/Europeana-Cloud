package eu.europeana.cloud.common.response;

import eu.europeana.cloud.common.model.File;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Representation of a record in specific version.
 */
@XmlRootElement
public class RepresentationRevisionResponse {

    /**
     * Identifier (cloud id) of a record this object is representation of.
     */
    private String cloudId;

    /**
     * Representation Name of this representation.
     */
    private String representationName;

    /**
     * Identifier of a version of this representation.
     */
    private String version;


    /**
     * A list of files which constitute this representation.
     */
    private List<File> files = new ArrayList<File>(0);


    /**
     * Revision identifier.
     */
    private String revisionId;

    public RepresentationRevisionResponse() {
        super();
    }

    /**
     * Creates a new instance of this class.
     *
     * @param cloudId
     * @param representationName
     * @param version
     * @param revisionId
     */
    public RepresentationRevisionResponse(String cloudId, String representationName, String version, String revisionId) {
        this.cloudId = cloudId;
        this.representationName = representationName;
        this.version = version;
        this.revisionId = revisionId;
    }

    /**
     * Creates a new instance of this class.
     *
     * @param cloudId
     * @param representationName
     * @param version
     * @param files
     * @param revisionId
     */
    public RepresentationRevisionResponse(String cloudId, String representationName, String version,
                                          List<File> files, String revisionId) {
        this.cloudId = cloudId;
        this.representationName = representationName;
        this.version = version;
        this.files = files;
        this.revisionId = revisionId;
    }


    /**
     * Creates a new instance of this class.
     *
     * @param representation
     */
    public RepresentationRevisionResponse(final RepresentationRevisionResponse representation) {
        this(representation.getCloudId(), representation.getRepresentationName(), representation.getVersion(),
                cloneFiles(representation), representation.getRevisionId());
    }


    private static List<File> cloneFiles(RepresentationRevisionResponse representation) {
        List<File> files = new ArrayList<>(representation.getFiles().size());
        for (File file : representation.getFiles()) {
            files.add(new File(file));
        }
        return files;
    }


    public String getCloudId() {
        return cloudId;
    }


    public void setCloudId(String cloudId) {
        this.cloudId = cloudId;
    }


    public String getRepresentationName() {
        return representationName;
    }


    public void setRepresentationName(String representationName) {
        this.representationName = representationName;
    }


    public String getVersion() {
        return version;
    }


    public void setVersion(String version) {
        this.version = version;
    }


    public List<File> getFiles() {
        return files;
    }


    public void setFiles(List<File> files) {
        this.files = files;
    }


    public String getRevisionId() {
        return revisionId;
    }


    public void setRevisionId(String revisionId) {
        this.revisionId = revisionId;
    }

    /**
     * This method is required for @PostFilter (Spring ACL) at RepresentationsResource.getRepresentations()
     */
    public String getId() {
        return getACLId();
    }

    private String getACLId() {
        return this.getCloudId() + "/" + this.getRepresentationName() + "/" + this.getVersion();
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 37 * hash + Objects.hashCode(this.cloudId);
        hash = 37 * hash + Objects.hashCode(this.representationName);
        hash = 37 * hash + Objects.hashCode(this.version);
        hash = 37 * hash + Objects.hashCode(this.files);
        hash = 37 * hash + Objects.hashCode(this.revisionId);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final RepresentationRevisionResponse other = (RepresentationRevisionResponse) obj;
        if (!Objects.equals(this.cloudId, other.cloudId)) {
            return false;
        }
        if (!Objects.equals(this.representationName, other.representationName)) {
            return false;
        }
        if (!Objects.equals(this.version, other.version)) {
            return false;
        }
        if (!Objects.equals(this.files, other.files)) {
            return false;
        }
        if (!Objects.equals(this.revisionId, other.revisionId)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "RepresentationRevisionResponse{" + "cloudId=" + cloudId + ", representationName=" + representationName + ", version="
                + version + ", files=" + files + ", revisionId=" + revisionId + '}';
    }
}
