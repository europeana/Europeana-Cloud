package eu.europeana.cloud.service.dps;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.xml.bind.annotation.XmlRootElement;

import java.util.HashMap;
import java.util.Map;

@XmlRootElement()
public class DpsTask implements Serializable {

	/* Dataset Key */
	public static final String DATASETS = "DATASETS";
	
	/* File URL Key */
	public static final String FILE_URLS = "FILE_URLS";

	/* List of input data (cloud-records or cloud-datasets) */
	private Map<String, List<String>> inputData;
	
	/* List of parameters (specific for each dps-topology) */
	private Map<String, String> parameters;

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

	public void addDataEntry(String dataType, List<String> data) {  
            inputData.put(dataType, data);
	}
        
        public List<String> getDataEntry(String dataType) { 
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
	public Map<String, List<String>> getInputData() {
		return inputData;
	}

	public void setInputData(Map<String, List<String>> inputData) {
            this.inputData = inputData;
	}
}
