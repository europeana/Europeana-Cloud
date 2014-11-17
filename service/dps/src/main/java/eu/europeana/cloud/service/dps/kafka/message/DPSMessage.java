package eu.europeana.cloud.service.dps.kafka.message;

import java.util.ArrayList;
import java.util.List;

public class DPSMessage {
	
	public List<String> data;
	public List<String> attributes;
	
	public DPSMessage() {
		data = new ArrayList<String>();
		attributes = new ArrayList<String>();
	}
	
	public void writeDataEntry(String _data) {
		data.add(_data);
		return;
	}
	
	public void writeAttributeEntry(String _attributes) {
		attributes.add(_attributes);
		return;
	}
	
	public String getDataEntry() {
		if (data.size() != 0)
			return data.get(data.size() - 1);
		return "";
	}
	
	public String getAttributesEntry() {
		if (attributes.size() != 0)
			return attributes.get(data.size() - 1);
		return "";
	}
	
	public List<String> getData() {
		return data;
	}
	
	public List<String> getAttributes() {
		return attributes;
	}
}