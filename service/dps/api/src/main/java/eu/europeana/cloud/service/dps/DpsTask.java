package eu.europeana.cloud.service.dps;

import com.google.common.base.Objects;
import eu.europeana.cloud.common.model.Revision;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.*;

@XmlRootElement()
public class DpsTask implements Serializable {


    /* Map of input data:
    cloud-records - InputDataType.FILE_URLS
    cloud-datasets - InputDataType.DATASET_URLS
    cloud-repositoryurl - InputDataType.REPOSITORY_URLS
    */
    private Map<InputDataType, List<String>> inputData;

    /* List of parameters (specific for each dps-topology) */
    private Map<String, String> parameters;

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

    /** Details of harvesting process */
    private OAIPMHHarvestingDetails harvestingDetails;


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

    public Revision getOutputRevision() {
        return outputRevision;
    }

    public void setOutputRevision(Revision outputRevision) {
        this.outputRevision = outputRevision;
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

    public OAIPMHHarvestingDetails getHarvestingDetails() {
        return harvestingDetails;
    }

    public void setHarvestingDetails(OAIPMHHarvestingDetails harvestingDetails) {
        this.harvestingDetails = harvestingDetails;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DpsTask)) return false;
        DpsTask dpsTask = (DpsTask) o;
        return taskId == dpsTask.taskId &&
                com.google.common.base.Objects.equal(inputData, dpsTask.inputData) &&
                Objects.equal(parameters, dpsTask.parameters) &&
                Objects.equal(outputRevision, dpsTask.outputRevision) &&
                Objects.equal(startTime, dpsTask.startTime) &&
                Objects.equal(createTime, dpsTask.createTime) &&
                Objects.equal(endTime, dpsTask.endTime) &&
                Objects.equal(taskName, dpsTask.taskName) &&
                Objects.equal(harvestingDetails, dpsTask.harvestingDetails);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(inputData, parameters, outputRevision, startTime, createTime, endTime, taskId, taskName, harvestingDetails);
    }
}

