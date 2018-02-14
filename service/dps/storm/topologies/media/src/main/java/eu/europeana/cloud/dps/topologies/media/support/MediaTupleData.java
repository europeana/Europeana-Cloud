package eu.europeana.cloud.dps.topologies.media.support;

import java.io.File;
import java.io.Serializable;
import java.net.InetAddress;
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
		private File content;
		private final InetAddress contentSource;
		private final String mimeType;
		private final String url;
		private Set<UrlType> types;
		
		public FileInfo(String url, String mimeType, File content, InetAddress contentSource) {
			this.url = url;
			this.mimeType = mimeType;
			this.content = content;
			this.contentSource = contentSource;
		}
		
		public File getContent() {
			return content;
		}
		
		void setContent(File content) {
			this.content = content;
		}
		
		public InetAddress getContentSource() {
			return contentSource;
		}
		
		public String getMimeType() {
			return mimeType;
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
	
	private List<FileInfo> fileInfos = new ArrayList<>();
	private Document edm;
	
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
