package eu.europeana.cloud.service.dps.xslt;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.common.collect.Maps;

public class StormTask implements Serializable {

	private String fileUrl;
	private String fileData;
	private HashMap<String, String> parameters;
	
	public StormTask() {
		this.parameters = Maps.newHashMap();
	}

	public StormTask(String fileUrl, HashMap<String, String> parameters) {
		this.fileUrl = fileUrl;
		fileData = new String();
		this.parameters = parameters;
	}

	public StormTask(String fileUrl, String fileData, HashMap<String, String> parameters) {
		this.fileUrl = fileUrl;
		this.fileData = fileData;
		this.parameters = parameters;
	}
	
	public String getFileUrl() {
		return fileUrl;
	}
	
	public String getFileByteData() {
		return fileData;
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
	
	public static StormTask fromStormTuple(Tuple tuple) {
		
		 return new StormTask(
				 tuple.getStringByField(StormTupleKeys.INPUT_FILES_TUPLE_KEY), 
				 	tuple.getStringByField(StormTupleKeys.FILE_CONTENT_TUPLE_KEY), 
				 		(HashMap<String, String>) tuple.getValueByField(StormTupleKeys.PARAMETERS_TUPLE_KEY));
	}
	
	public Values toStormTuple() {
		return new Values(fileUrl, fileData, parameters);
	}
}