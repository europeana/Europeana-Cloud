package eu.europeana.cloud.dps.topologies.media.support;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

@DefaultSerializer(JavaSerializer.class)
public class StatsTupleData implements Serializable {
	
	public static class Status implements Serializable {
		/** Special value for {@link Status} message if there was no error */
		public static final String STATUS_OK = "OK";
		
		public final String resourceUrl;
		public final String message;
		public final Date date;
		
		/**
		 * @param message error message or {@link #STATUS_OK} if there was no error.
		 */
		public Status(String resourceUrl, String message, Date date) {
			this.resourceUrl = resourceUrl;
			this.message = message;
			this.date = date;
		}
		
		public boolean isError() {
			return !STATUS_OK.equals(message);
		}
	}
	
	public static final String FIELD_NAME = "mediaTopology.statsData";
	public static final String STREAM_ID = "stats";
	
	private final long taskId;
	private final int resourceCount;
	private HashMap<String, Status> status = new HashMap<>();
	
	private long downloadStartTime;
	private long downloadEndTime;
	private long downloadedBytes;
	
	private long processingStartTime;
	private long processingEndTime;
	
	private long uploadStartTime;
	private long uploadEndTime;
	
	public StatsTupleData(long taskId, int resourceCount) {
		this.taskId = taskId;
		this.resourceCount = resourceCount;
	}
	
	public long getTaskId() {
		return taskId;
	}
	
	public int getResourceCount() {
		return resourceCount;
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
	
	public void addStatus(String resourceUrl, String message) {
		status.put(resourceUrl, new Status(resourceUrl, message, new Date()));
	}
	
	public void addErrorIfAbsent(String resourceUrl, String message) {
		Status old = status.get(resourceUrl);
		if (old == null || !old.isError())
			status.put(resourceUrl, new Status(resourceUrl, message, new Date()));
	}
	
	public List<Status> getStatuses() {
		return new ArrayList<>(status.values());
	}
	
	public List<Status> getErrors() {
		return status.values().stream().filter(Status::isError).collect(Collectors.toList());
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
	
	public void setUploadStartTime(long uploadStartTime) {
		this.uploadStartTime = uploadStartTime;
	}
	
	public long getUploadStartTime() {
		return uploadStartTime;
	}
	
	public void setUploadEndTime(long uploadEndTime) {
		this.uploadEndTime = uploadEndTime;
	}
	
	public long getUploadEndTime() {
		return uploadEndTime;
	}
}
