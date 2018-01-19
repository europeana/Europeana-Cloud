package eu.europeana.cloud.dps.topologies.media.support;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.w3c.dom.Document;

import eu.europeana.cloud.common.model.Representation;

public class MediaTupleData implements Serializable {
	
	public enum UrlType {
		OBJECT("edm:object"),
		HAS_VIEW("edm:hasView"),
		IS_SHOWN_BY("edm:isShownBy"),
		IS_SHOWN_AT("edm:isShownAt");
		
		public final String tagName;
		
		UrlType(String tagName) {
			this.tagName = tagName;
		}
	}
	
	public static class FileInfo implements Serializable {
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
	
	public static final String FIELD_NAME = "mediaTopology.mediaData";
	
	private long taskId;
	private Representation edmRepresentation;
	
	private List<FileInfo> fileInfos = new ArrayList<>();
	private Document edm;
	
	public MediaTupleData(long taskId) {
		this.taskId = taskId;
	}
	
	public long getTaskId() {
		return taskId;
	}
	
	public Representation getEdmRepresentation() {
		return edmRepresentation;
	}
	
	public void setEdmRepresentation(Representation edmRepresentation) {
		this.edmRepresentation = edmRepresentation;
	}
	
	public List<FileInfo> getFileInfos() {
		return fileInfos;
	}
	
	public void addFileInfo(FileInfo fileInfo) {
		fileInfos.add(fileInfo);
	}
	
	public Document getEdm() {
		return edm;
	}
	
	public void setEdm(Document edm) {
		this.edm = edm;
	}
	
}
