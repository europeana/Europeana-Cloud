package eu.europeana.cloud.common.model;

import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

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

	private Date creationDate;

	private boolean persistent;


	public Representation() {
		super();
	}


	public Representation(String recordId, String schema, String version,
			URI allVersionsUri, URI uri, String dataProvider, List<File> files,
			boolean persistent, Date creationDate) {
		super();
		this.recordId = recordId;
		this.schema = schema;
		this.version = version;
		this.allVersionsUri = allVersionsUri;
		this.uri = uri;
		this.dataProvider = dataProvider;
		this.files = files;
		this.persistent = persistent;
		this.creationDate = creationDate;
	}


	public Representation(final Representation representation) {
		this(representation.getRecordId(), representation.getSchema(), representation.getVersion(),
				representation.getAllVersionsUri(), representation.getUri(), representation.getDataProvider(),
				cloneFiles(representation), representation.isPersistent(), representation.getCreationDate());
	}


	private static List<File> cloneFiles(Representation representation) {
		List<File> files = new ArrayList<>(representation.getFiles().size());
		for (File file : representation.getFiles()) {
			files.add(new File(file));
		}
		return files;
	}


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


	public Date getCreationDate() {
		return creationDate;
	}


	public void setCreationDate(Date creationDate) {
		this.creationDate = creationDate;
	}


	@Override
	public int hashCode() {
		int hash = 7;
		hash = 37 * hash + Objects.hashCode(this.recordId);
		hash = 37 * hash + Objects.hashCode(this.schema);
		hash = 37 * hash + Objects.hashCode(this.version);
		hash = 37 * hash + Objects.hashCode(this.dataProvider);
		hash = 37 * hash + Objects.hashCode(this.files);
		hash = 37 * hash + Objects.hashCode(this.creationDate);
		hash = 37 * hash + (this.persistent ? 1 : 0);
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
		final Representation other = (Representation) obj;
		if (!Objects.equals(this.recordId, other.recordId)) {
			return false;
		}
		if (!Objects.equals(this.schema, other.schema)) {
			return false;
		}
		if (!Objects.equals(this.version, other.version)) {
			return false;
		}
		if (!Objects.equals(this.dataProvider, other.dataProvider)) {
			return false;
		}
		if (!Objects.equals(this.files, other.files)) {
			return false;
		}
		if (!Objects.equals(this.creationDate, other.creationDate)) {
			return false;
		}
		if (this.persistent != other.persistent) {
			return false;
		}
		return true;
	}


	@Override
	public String toString() {
		return "Representation{" + "recordId=" + recordId + ", schema=" + schema + ", version=" + version
				+ ", dataProvider=" + dataProvider + ", files=" + files + ", creationDate="
				+ creationDate + ", persistent=" + persistent + '}';
	}
}
