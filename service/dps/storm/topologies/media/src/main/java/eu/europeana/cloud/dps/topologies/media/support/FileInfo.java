package eu.europeana.cloud.dps.topologies.media.support;

import java.io.Serializable;
import java.util.Set;

import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData.UrlType;

public class FileInfo implements Serializable {
	private byte[] content;
	private String mimeType;
	private String url;
	private Set<UrlType> types;
	
	public FileInfo(String url, String mimeType, byte[] content) {
		this.url = url;
		this.mimeType = mimeType;
		this.content = content;
	}
	
	public byte[] getContent() {
		return content;
	}
	
	public String getMimeType() {
		return mimeType;
	}
	
	public int getLength() {
		return content.length;
	}
	
	public String getUrl() {
		return url;
	}
	
	public Set<UrlType> getTypes() {
		return types;
	}
	
	public void setTypes(Set<UrlType> types) {
		this.types = types;
	}
	
}
