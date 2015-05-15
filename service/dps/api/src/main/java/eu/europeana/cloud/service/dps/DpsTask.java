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

	// INPUT DATA TYPES
	public static final String DATASETS = "DATASETS";
	public static final String FILE_URLS = "FILE_URLS";

	private Map<String, List<String>> inputData;
	private Map<String, String> parameters;

	private Date startTime = null;
	private Date createTime = new Date(System.currentTimeMillis());
	private Date endTime = null;
	
	private long taskId;
	private String taskName;

	public DpsTask() {
            this("");
	}
	
	public DpsTask(String taskName) {
		
            this.taskName = taskName;

            inputData = new HashMap();
            parameters = new HashMap();

            taskId = UUID.randomUUID().getMostSignificantBits();
	}
	
        private void setTaskId(long taskId) {
        this.taskId = taskId;
	}
                
	public long getTaskId() {
            return taskId;
	}
        
	public void setTaskName(String taskName) {
            this.taskName = taskName;
	}
	
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

	public Map<String, String> getParameters() {
            return parameters;
	}

	public void setParameters(Map<String, String> parameters) {
            this.parameters = parameters;
	}

	public Map<String, List<String>> getInputData() {  
            return inputData;
	}

	public void setInputData(Map<String, List<String>> inputData) {
            this.inputData = inputData;
	}
}
