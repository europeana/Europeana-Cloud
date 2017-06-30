package eu.europeana.cloud.service.dps;

import eu.europeana.cloud.common.model.Revision;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.*;

@XmlRootElement()
public class DpsTask implements Serializable {


    /* Map of input data:
    cloud-records - InputDataType.FILE_URLS
    cloud-datasets InputDataType.DATASET_URLS
    */
    private Map<InputDataType, List<String>> inputData;

    /* List of parameters (specific for each dps-topology) */
    private Map<String, String> parameters;

    public Revision getOutputRevision() {
        return outputRevision;
    }

    public void setOutputRevision(Revision outputRevision) {
        this.outputRevision = outputRevision;
    }

    /* output revision*/
    private Revision outputRevision;

    /* Task start time */
    private Date startTime = null;

    /* Task create time */
    private Date createTime = new Date(System.currentTimeMillis());

    /* Task end time*/
    private Date endTime = null;

    /* Unique id for this task */
    private long taskId;

    /* Name for the task */
    private String taskName;


    public DpsTask() {
        this("");
    }

    /**
     * @param taskName
     */
    public DpsTask(String taskName) {

        this.taskName = taskName;

        inputData = new HashMap();
        parameters = new HashMap();

        taskId = UUID.randomUUID().getMostSignificantBits();
    }

    /**
     * @return Unique id for this task
     */
    public long getTaskId() {
        return taskId;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    /**
     * @return Name for the task
     */
    public String getTaskName() {
        return taskName;
    }

    public void addDataEntry(InputDataType dataType, List<String> data) {
        inputData.put(dataType, data);
    }

    public List<String> getDataEntry(InputDataType dataType) {
        return inputData.get(dataType);
    }

    public void addParameter(String parameterKey, String parameterValue) {
        parameters.put(parameterKey, parameterValue);
    }

    public String getParameter(String parameterKey) {
        return parameters.get(parameterKey);
    }

    /**
     * @return List of parameters (specific for each dps-topology)
     */
    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    /**
     * @return List of input data (cloud-records or cloud-datasets)
     */
    public Map<InputDataType, List<String>> getInputData() {
        return inputData;
    }

    public void setInputData(Map<InputDataType, List<String>> inputData) {
        this.inputData = inputData;
    }
}

