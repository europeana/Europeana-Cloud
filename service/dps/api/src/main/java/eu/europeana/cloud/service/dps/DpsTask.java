package eu.europeana.cloud.service.dps;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import com.google.common.collect.Maps;

@XmlRootElement()
public class DpsTask implements Serializable {

	// INPUT DATA TYPES
	public static final String DATASETS = "DATASETS";
	public static final String FILE_URLS = "FILE_URLS";
	
	 HashMap<String, List<String>> inputData;
	 HashMap<String, String> parameters;
	
	public DpsTask() {
		inputData = Maps.newHashMap();
		parameters = Maps.newHashMap();
	}
	
	public void addDataEntry(String dataType, List<String> data) {
		inputData.put(dataType, data);
	}
	
	public void addParameter(String parameterKey, String parameterValue) {
		parameters.put(parameterKey, parameterValue);
	}

	public List<String> getDataEntry(String dataType) {
		return inputData.get(dataType);
	}
	
	public String getParameter(String parameterKey) {
		return parameters.get(parameterKey);
	}
	
	public HashMap<String, String> getParameters() {
		return parameters;
	}
	
	public void setParameters(HashMap<String, String> parameters) {
		this.parameters = parameters;
	}
	
	public HashMap<String, List<String>> getInputData() {
		return inputData;
	}
	
	public void setInputData(HashMap<String, List<String>> inputData) {
		this.inputData = inputData;
	}
}