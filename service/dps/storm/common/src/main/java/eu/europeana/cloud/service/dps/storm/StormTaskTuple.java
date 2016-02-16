package eu.europeana.cloud.service.dps.storm;

import backtype.storm.tuple.Fields;

import java.io.*;
import java.util.HashMap;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;


/**
 * Storm Tuple that is aware of the DpsTask it is part of.
 * <p/>
 * Useful to track progress of a Task as tuples emitted from different tasks are
 * being processed.
 *
 * @author manos
 */
public class StormTaskTuple implements Serializable {

    private String fileUrl;
    private byte[] fileData;
    private long taskId;
    private String taskName;
    private Map<String, String> parameters;


    private static final int BATCH_MAX_SIZE=10240;
    public StormTaskTuple() {

        this.taskName = "";
        this.parameters = new HashMap<>();
    }

    public StormTaskTuple(long taskId, String taskName, String fileUrl,
                          byte[] fileData, Map<String, String> parameters) {

        this.taskId = taskId;
        this.taskName = taskName;
        this.fileUrl = fileUrl;
        this.fileData = fileData;
        this.parameters = parameters;
    }

    public String getFileUrl() {
        return fileUrl;
    }


    public ByteArrayInputStream getFileByteDataAsStream() {
        if (fileData != null) {
            return new ByteArrayInputStream(fileData);
        } else {
            return null;
        }
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

    public void setFileData(byte[] fileData) {
        this.fileData = fileData;
    }

    public void setFileData(InputStream is) throws IOException {

        if (is != null) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            int read = 0;
            byte[] bytes = new byte[BATCH_MAX_SIZE];
            while ((read = is.read(bytes)) != -1) {
                buffer.write(bytes, 0, read);
            }
            this.fileData = buffer.toByteArray();
        } else {
            this.fileData = null;
        }

    }

    public byte[] getFileData() {
        return fileData;
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

    public Map<String, String> getParameters() {
        return parameters;
    }

    public static StormTaskTuple fromStormTuple(Tuple tuple) {

        return new StormTaskTuple(
                tuple.getLongByField(StormTupleKeys.TASK_ID_TUPLE_KEY),
                tuple.getStringByField(StormTupleKeys.TASK_NAME_TUPLE_KEY),
                tuple.getStringByField(StormTupleKeys.INPUT_FILES_TUPLE_KEY),
                tuple.getBinaryByField(StormTupleKeys.FILE_CONTENT_TUPLE_KEY),
                (HashMap<String, String>) tuple
                        .getValueByField(StormTupleKeys.PARAMETERS_TUPLE_KEY));
    }

    public Values toStormTuple() {
        return new Values(taskId, taskName, fileUrl, fileData, parameters);
    }

    public static Fields getFields() {
        return new Fields(
                StormTupleKeys.TASK_ID_TUPLE_KEY,
                StormTupleKeys.TASK_NAME_TUPLE_KEY,
                StormTupleKeys.INPUT_FILES_TUPLE_KEY,
                StormTupleKeys.FILE_CONTENT_TUPLE_KEY,
                StormTupleKeys.PARAMETERS_TUPLE_KEY);
    }
}
