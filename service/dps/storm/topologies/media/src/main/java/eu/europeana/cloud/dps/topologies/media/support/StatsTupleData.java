package eu.europeana.cloud.dps.topologies.media.support;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class StatsTupleData implements Serializable {
	
	public static final String FIELD_NAME = "mediaTopology.statsData";
	public static final String STREAM_ID = "stats";
	
	private final long taskId;
	private final int resourceNumber;
	private ArrayList<String> errors = new ArrayList<>();
	
	private long downloadStartTime;
	private long downloadEndTime;
	private long downloadedBytes;
	
	private long processingStartTime;
	private long processingEndTime;
	
	public StatsTupleData(long taskId, int resourceNumber) {
		this.taskId = taskId;
		this.resourceNumber = resourceNumber;
	}
	
	public long getTaskId() {
		return taskId;
	}
	
	public int getResourceNumber() {
		return resourceNumber;
	}
	
	public void setDownloadStartTime(long downloadStartTime) {
		this.downloadStartTime = downloadStartTime;
	}
	
	public long getDownloadStartTime() {
		return downloadStartTime;
	}
	
	public void setDownloadEndTime(long downloadEndTime) {
		this.downloadEndTime = downloadEndTime;
	}
	
	public long getDownloadEndTime() {
		return downloadEndTime;
	}
	
	public void setDownloadedBytes(long downloadedBytes) {
		this.downloadedBytes = downloadedBytes;
	}
	
	public long getDownloadedBytes() {
		return downloadedBytes;
	}
	
	public void addError(String error) {
		errors.add(error);
	}
	
	public List<String> getErrors() {
		return errors;
	}
	
	public void setProcessingStartTime(long processingStartTime) {
		this.processingStartTime = processingStartTime;
	}
	
	public long getProcessingStartTime() {
		return processingStartTime;
	}
	
	public void setProcessingEndTime(long processingEndTime) {
		this.processingEndTime = processingEndTime;
	}
	
	public long getProcessingEndTime() {
		return processingEndTime;
	}
}
