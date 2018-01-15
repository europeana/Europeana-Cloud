package eu.europeana.cloud.dps.topologies.media.support;

import java.io.Serializable;

public class StatsTupleData implements Serializable {
	
	public static final String FIELD_NAME = "mediaTopology.statsData";
	public static final String STREAM_ID = "stats";
	
	private long taskId;
	private String error;
	private long length;
	private long time;
	
	public StatsTupleData(long taskId, String error) {
		this.taskId = taskId;
		this.error = error;
	}
	
	public StatsTupleData(long taskId, long length, long time) {
		this.taskId = taskId;
		this.length = length;
		this.time = time;
	}
	
	public long getTaskId() {
		return taskId;
	}
	
	public String getError() {
		return error;
	}
	
	public long getLength() {
		return length;
	}
	
	public long getTime() {
		return time;
	}
	
}
