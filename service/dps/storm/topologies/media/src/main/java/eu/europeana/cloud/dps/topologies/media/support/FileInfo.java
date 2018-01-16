package eu.europeana.cloud.dps.topologies.media.support;

import java.io.Serializable;
import java.util.Set;

import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData.UrlType;

public class FileInfo implements Serializable {
	private byte[] content;
	private String mimeType;
	private String url;
	private Set<UrlType> types;
	
	public FileInfo(String fileUrl) {
		url = fileUrl;
	}
	
	public byte[] getContent() {
		return content;
	}
	
	public void setContent(byte[] content) {
		this.content = content;
	}
	
	public String getMimeType() {
		return mimeType;
	}
	
	public void setMimeType(String mimeType) {
		this.mimeType = mimeType;
	}
	
	public int getLength() {
		return content.length;
	}
	
	public String getUrl() {
		return url;
	}
	
	public boolean isEmpty() {
		return content == null;
	}
	
	public Set<UrlType> getTypes() {
		return types;
	}
	
	public void setTypes(Set<UrlType> types) {
		this.types = types;
	}
	
}
