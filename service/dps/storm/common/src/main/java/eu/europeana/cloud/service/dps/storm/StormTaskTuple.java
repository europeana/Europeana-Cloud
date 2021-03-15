package eu.europeana.cloud.service.dps.storm;


import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.io.IOUtils;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.*;

import static eu.europeana.cloud.service.dps.storm.StormTupleKeys.*;


/**
 * Storm Tuple that is aware of the DpsTask it is part of.
 * <p/>
 * Useful to track progress of a Task as tuples emitted from different tasks are
 * being processed.
 *
 * @author manos
 */
@Setter
@Getter
public class StormTaskTuple implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final int BATCH_MAX_SIZE = 1024 * 4;

    private String fileUrl;
    private byte[] fileData;
    private long taskId;
    private String taskName;
    private Map<String, String> parameters;
    private Revision revisionToBeApplied;
    private OAIPMHHarvestingDetails sourceDetails;
    private int recordAttemptNumber;
    private List<String> identifiersToUse;

    public StormTaskTuple() {
        this(0L, "", null, null, new HashMap<>(), null);
    }

    public StormTaskTuple(long taskId, String taskName, String fileUrl,
                          byte[] fileData, Map<String, String> parameters, Revision revisionToBeApplied) {
        this(taskId, taskName, fileUrl, fileData, parameters, revisionToBeApplied, null);
    }

    public StormTaskTuple(long taskId, String taskName, String fileUrl,
                          byte[] fileData, Map<String, String> parameters, Revision revisionToBeApplied,
                          OAIPMHHarvestingDetails sourceDetails) {
        this(taskId, taskName, fileUrl, fileData, parameters, revisionToBeApplied, sourceDetails, 0, new ArrayList<>());
    }

    public StormTaskTuple(long taskId, String taskName, String fileUrl,
                          byte[] fileData, Map<String, String> parameters, Revision revisionToBeApplied,
                          OAIPMHHarvestingDetails sourceDetails, int recordAttemptNumber,
                          List<String> localIdentifiers) {
        this.taskId = taskId;
        this.taskName = taskName;
        this.fileUrl = fileUrl;
        this.fileData = fileData;
        this.parameters = parameters;
        this.revisionToBeApplied = revisionToBeApplied;
        this.sourceDetails = sourceDetails;
        this.recordAttemptNumber = recordAttemptNumber;
        this.identifiersToUse = localIdentifiers;
    }

    public ByteArrayInputStream getFileByteDataAsStream() {
        if (fileData != null) {
            return new ByteArrayInputStream(fileData);
        } else {
            return null;
        }
    }

    public void setFileData(byte[] fileData) {
        this.fileData = fileData;
    }

    public void setFileData(InputStream is) throws IOException {
        try(ByteArrayOutputStream tempByteArrayOutputStream = new ByteArrayOutputStream()) {
            if (is != null) {
                byte[] buffer = new byte[BATCH_MAX_SIZE];
                IOUtils.copyLarge(is, tempByteArrayOutputStream, buffer);
                this.fileData = tempByteArrayOutputStream.toByteArray();
            } else {
                this.fileData = null;
            }
        } finally {
            //NOTE: is should be closed outside setFileData method or this method should named setFileDataAndClose
            if (is != null) {
                is.close();
            }
        }
    }

    public void addParameter(String parameterKey, String parameterValue) {
        parameters.put(parameterKey, parameterValue);
    }

    public String getParameter(String parameterKey) {
        return parameters.get(parameterKey);
    }

    public boolean hasRevisionToBeApplied() {
        return revisionToBeApplied != null;
    }

    public static StormTaskTuple fromStormTuple(Tuple tuple) {

        return new StormTaskTuple(
                tuple.getLongByField(TASK_ID_TUPLE_KEY),
                tuple.getStringByField(TASK_NAME_TUPLE_KEY),
                tuple.getStringByField(INPUT_FILES_TUPLE_KEY),
                tuple.getBinaryByField(FILE_CONTENT_TUPLE_KEY),
                (HashMap<String, String>)tuple.getValueByField(PARAMETERS_TUPLE_KEY),
                (Revision) tuple.getValueByField(REVISIONS),
                (OAIPMHHarvestingDetails) tuple.getValueByField(SOURCE_TO_HARVEST),
                tuple.getIntegerByField(RECORD_ATTEMPT_NUMBER),
                (List<String>)tuple.getValueByField(LOCAL_IDENTIFIERS));

    }

    public static StormTaskTuple fromValues(List<Object> list) {

        return new StormTaskTuple(
                (Long) list.get(0),
                (String) list.get(1),
                (String) list.get(2),
                (byte[]) list.get(3),
                (Map<String, String>) list.get(4),
                (Revision) list.get(5),
                (OAIPMHHarvestingDetails) list.get(6),
                (Integer) list.get(7),
                (ArrayList<String>)list.get(8));

    }

    public Values toStormTuple() {
        return new Values(taskId, taskName, fileUrl, fileData, parameters, revisionToBeApplied, sourceDetails, recordAttemptNumber, identifiersToUse);
    }

    public static Fields getFields() {
        return new Fields(
                TASK_ID_TUPLE_KEY,
                TASK_NAME_TUPLE_KEY,
                INPUT_FILES_TUPLE_KEY,
                FILE_CONTENT_TUPLE_KEY,
                PARAMETERS_TUPLE_KEY,
                REVISIONS,
                SOURCE_TO_HARVEST,
                RECORD_ATTEMPT_NUMBER,
                LOCAL_IDENTIFIERS);
    }

    public boolean isMarkedAsDeleted() {
        return "true".equals(parameters.get(PluginParameterKeys.MARKED_AS_DELETED));
    }

    public void setMarkedAsDeleted(boolean markedAsDeleted) {
        if(markedAsDeleted){
            parameters.put(PluginParameterKeys.MARKED_AS_DELETED,"true");
        }else{
            parameters.remove(PluginParameterKeys.MARKED_AS_DELETED);
        }
    }

}