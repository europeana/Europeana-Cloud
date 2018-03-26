package eu.europeana.cloud.dps.topologies.media.support;

import java.io.File;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.metis.mediaservice.EdmObject;

public class MediaTupleData implements Serializable {
	
	public static class FileInfo implements Serializable {
		private final String url;
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
	}
	
	public static final String FIELD_NAME = "mediaTopology.mediaData";
	
	final private long taskId;
	final private Representation edmRepresentation;
	
	private EdmObject edm;
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
	
	public EdmObject getEdm() {
		return edm;
	}
	
	public void setEdm(EdmObject edm) {
		this.edm = edm;
	}
	
	public Map<String, Integer> getConnectionLimitsPerSource() {
		return connectionLimitsPerSource;
	}
	
	public void setConnectionLimitsPerSource(Map<String, Integer> connectionLimitsPerSource) {
		this.connectionLimitsPerSource = connectionLimitsPerSource;
	}
}
