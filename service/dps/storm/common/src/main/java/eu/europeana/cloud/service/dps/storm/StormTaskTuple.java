package eu.europeana.cloud.service.dps.storm;


import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import org.apache.commons.io.IOUtils;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.HashMap;
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
    private static final int BATCH_MAX_SIZE = 1024 * 4;
    private Revision revisionToBeApplied;
    private OAIPMHHarvestingDetails sourceDetails;

    public StormTaskTuple() {

        this.taskName = "";
        this.parameters = new HashMap<>();
    }

    public StormTaskTuple(long taskId, String taskName, String fileUrl,
                          byte[] fileData, Map<String, String> parameters, Revision revision) {

        this.taskId = taskId;
        this.taskName = taskName;
        this.fileUrl = fileUrl;
        this.fileData = fileData;
        this.parameters = parameters;
        revisionToBeApplied = revision;
    }

    public StormTaskTuple(long taskId, String taskName, String fileUrl,
                          byte[] fileData, Map<String, String> parameters, Revision revision, OAIPMHHarvestingDetails sourceDetails) {
        this(taskId, taskName, fileUrl, fileData, parameters, revision);
        this.sourceDetails = sourceDetails;
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
        ByteArrayOutputStream tempByteArrayOutputStream = null;
        try {
            if (is != null) {
                tempByteArrayOutputStream = new ByteArrayOutputStream();
                byte[] buffer = new byte[BATCH_MAX_SIZE];
                IOUtils.copyLarge(is, tempByteArrayOutputStream, buffer);
                this.fileData = tempByteArrayOutputStream.toByteArray();
            } else {
                this.fileData = null;
            }
        } finally {
            if (is != null) {
                is.close();
            }
            if (tempByteArrayOutputStream != null) {
                tempByteArrayOutputStream.close();
            }
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

    public OAIPMHHarvestingDetails getSourceDetails() {
        return sourceDetails;
    }

    public void setSourceDetails(OAIPMHHarvestingDetails sourceDetails) {
        this.sourceDetails = sourceDetails;
    }

    public String getParameter(String parameterKey) {
        return parameters.get(parameterKey);
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public Revision getRevisionToBeApplied() {
        return revisionToBeApplied;
    }

    public void setRevisionToBeApplied(Revision revision) {
        revisionToBeApplied = revision;
    }

    public boolean hasRevisionToBeApplied() {
        return revisionToBeApplied != null;
    }

    public static StormTaskTuple fromStormTuple(Tuple tuple) {

        return new StormTaskTuple(
                tuple.getLongByField(StormTupleKeys.TASK_ID_TUPLE_KEY),
                tuple.getStringByField(StormTupleKeys.TASK_NAME_TUPLE_KEY),
                tuple.getStringByField(StormTupleKeys.INPUT_FILES_TUPLE_KEY),
                tuple.getBinaryByField(StormTupleKeys.FILE_CONTENT_TUPLE_KEY),
                (HashMap<String, String>) tuple
                        .getValueByField(StormTupleKeys.PARAMETERS_TUPLE_KEY),
                (Revision) tuple
                        .getValueByField(StormTupleKeys.REVISIONS),
                (OAIPMHHarvestingDetails) tuple.getValueByField(StormTupleKeys.SOURCE_TO_HARVEST));

    }

    public Values toStormTuple() {
        return new Values(taskId, taskName, fileUrl, fileData, parameters, revisionToBeApplied, sourceDetails);
    }

    public static Fields getFields() {
        return new Fields(
                StormTupleKeys.TASK_ID_TUPLE_KEY,
                StormTupleKeys.TASK_NAME_TUPLE_KEY,
                StormTupleKeys.INPUT_FILES_TUPLE_KEY,
                StormTupleKeys.FILE_CONTENT_TUPLE_KEY,
                StormTupleKeys.PARAMETERS_TUPLE_KEY,
                StormTupleKeys.REVISIONS,
                StormTupleKeys.SOURCE_TO_HARVEST);
    }


}