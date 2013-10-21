package eu.europeana.cloud.common.model;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * Record
 */
@XmlRootElement
public class Record {

	private String id;

	private List<Representation> representations;

	public Record() {
		super();
		this.id = null;
		this.representations = new ArrayList<>();
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<Representation> getRepresentations() {
		return representations;
	}

	public void setRepresentations(List<Representation> representations) {
		this.representations = representations;
	}

	@Override
	public String toString() {
		return "Record [id=" + id + ", representations=" + representations
				+ "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result
				+ ((representations == null) ? 0 : representations.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Record other = (Record) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (representations == null) {
			if (other.representations != null)
				return false;
		} else if (!representations.equals(other.representations))
			return false;
		return true;
	}

}
