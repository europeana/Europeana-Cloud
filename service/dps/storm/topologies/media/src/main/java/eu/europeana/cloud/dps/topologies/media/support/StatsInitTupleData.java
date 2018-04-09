package eu.europeana.cloud.dps.topologies.media.support;

import java.io.Serializable;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

@DefaultSerializer(JavaSerializer.class)
public class StatsInitTupleData implements Serializable {
	
	public static final String FIELD_NAME = "mediaTopology.statsInit";
	public static final String STREAM_ID = "stats-init";
	
	private final long taskId;
	private final long startTime;
	private final long edmCount;
	
	public StatsInitTupleData(long taskId, long startTime, long edmsCount) {
		this.taskId = taskId;
		this.startTime = startTime;
		this.edmCount = edmsCount;
	}
	
	public long getTaskId() {
		return taskId;
	}
	
	public long getStartTime() {
		return startTime;
	}
	
	public long getEdmCount() {
		return edmCount;
	}
}
