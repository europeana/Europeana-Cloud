package eu.europeana.cloud.dps.topologies.media.support;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

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
	
	public static final String FIELD_NAME = "mediaTopology.mediaData";
	
	private long taskId;
	private Representation edmRepresentation;
	
	private Map<UrlType, List<String>> fileUrls;
	private Map<String, byte[]> fileContents;
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
	
	public Map<UrlType, List<String>> getFileUrls() {
		return fileUrls;
	}
	
	public void setFileUrls(Map<UrlType, List<String>> fileUrls) {
		this.fileUrls = fileUrls;
	}
	
	public Map<String, byte[]> getFileContents() {
		return fileContents;
	}
	
	public void setFileContents(Map<String, byte[]> fileContents) {
		this.fileContents = fileContents;
	}
	
	public Document getEdm() {
		return edm;
	}
	
	public void setEdm(Document edm) {
		this.edm = edm;
	}
	
}
