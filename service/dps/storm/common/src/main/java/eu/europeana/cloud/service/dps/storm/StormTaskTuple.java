package eu.europeana.cloud.service.dps.storm;

import java.io.Serializable;
import java.util.HashMap;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.common.collect.Maps;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;

/**
 * Storm Tuple that is aware of the DpsTask it is part of.
 * 
 * Useful to track progress of a Task as tuples emitted from different tasks are
 * being processed.
 * 
 * @author manos
 */
public class StormTaskTuple implements Serializable {

	private String fileUrl;
	private String fileData;

	private long taskId;
	private String taskName;

	private HashMap<String, String> parameters;

	public StormTaskTuple() {

		this.taskName = "";
		this.parameters = Maps.newHashMap();
	}

	public StormTaskTuple(String fileUrl, HashMap<String, String> parameters) {

		fileData = new String();
		this.taskName = "";
		this.fileUrl = fileUrl;
		this.parameters = parameters;
	}

	public StormTaskTuple(long taskId, String taskName, String fileUrl,
			String fileData, HashMap<String, String> parameters) {

		this.taskId = taskId;
		this.taskName = taskName;
		this.fileUrl = fileUrl;
		this.fileData = fileData;
		this.parameters = parameters;
	}

	public String getFileUrl() {
		return fileUrl;
	}

	public String getFileByteData() {
		return Arrays.toString(Base64.decodeBase64(fileData));
	}
        
        public ByteArrayInputStream getFileByteDataAsStream() {
		return new ByteArrayInputStream(Base64.decodeBase64(fileData));
	}

	public long getTaskId() {
		return taskId;
	}

	public String getTaskName() {
		return taskName;
	}

	public void setTaskId(long taskId) {
		this.taskId = taskId;
	}

	public void setFileData(String fileData) {
		this.fileData = Base64.encodeBase64String(fileData.getBytes());
	}
        
        public void setFileData(InputStream is) throws IOException {
                this.fileData = Base64.encodeBase64String(IOUtils.toByteArray(is));
        }

	public void setFileUrl(String fileUrl) {
		this.fileUrl = fileUrl;
	}

	public void addParameter(String parameterKey, String parameterValue) {
		parameters.put(parameterKey, parameterValue);
	}

	public String getParameter(String parameterKey) {
		return parameters.get(parameterKey);
	}

	public HashMap<String, String> getParameters() {
		return parameters;
	}

	public static StormTaskTuple fromStormTuple(Tuple tuple) {

		return new StormTaskTuple(
				tuple.getLongByField(StormTupleKeys.TASK_ID_TUPLE_KEY),
				tuple.getStringByField(StormTupleKeys.TASK_NAME_TUPLE_KEY),
				tuple.getStringByField(StormTupleKeys.INPUT_FILES_TUPLE_KEY),
				tuple.getStringByField(StormTupleKeys.FILE_CONTENT_TUPLE_KEY),
				(HashMap<String, String>) tuple
						.getValueByField(StormTupleKeys.PARAMETERS_TUPLE_KEY));
	}

	public Values toStormTuple() {
		return new Values(taskId, taskName, fileUrl, fileData, parameters);
	}
}
