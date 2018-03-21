package eu.europeana.cloud.dps.topologies.media.support;

import java.io.File;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
		private final String url;
		private Set<UrlType> types = new HashSet<>();
		private File content;
		private String mimeType;
		private InetAddress contentSource;
		
		public FileInfo(String url) {
			this.url = url;
		}
		
		public File getContent() {
			return content;
		}
		
		public void setContent(File content) {
			this.content = content;
		}
		
		public InetAddress getContentSource() {
			return contentSource;
		}
		
		public void setContentSource(InetAddress contentSource) {
			this.contentSource = contentSource;
		}
		
		public String getMimeType() {
			return mimeType;
		}
		
		public void setMimeType(String mimeType) {
			this.mimeType = mimeType;
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
	
	final private long taskId;
	final private Representation edmRepresentation;
	
	private Document edm;
	private List<FileInfo> fileInfos;
	private Map<String, Integer> connectionLimitsPerSource;
	
	public MediaTupleData(long taskId, Representation edmRepresentation) {
		this.taskId = taskId;
		this.edmRepresentation = edmRepresentation;
	}
	
	public long getTaskId() {
		return taskId;
	}
	
	public Representation getEdmRepresentation() {
		return edmRepresentation;
	}
	
	public List<FileInfo> getFileInfos() {
		return fileInfos;
	}
	
	public void setFileInfos(List<FileInfo> fileInfos) {
		this.fileInfos = fileInfos;
	}
	
	public Document getEdm() {
		return edm;
	}
	
	public void setEdm(Document edm) {
		this.edm = edm;
	}
	
	public Map<String, Integer> getConnectionLimitsPerSource() {
		return connectionLimitsPerSource;
	}
	
	public void setConnectionLimitsPerSource(Map<String, Integer> connectionLimitsPerSource) {
		this.connectionLimitsPerSource = connectionLimitsPerSource;
	}
}
