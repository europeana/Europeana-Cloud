package eu.europeana.cloud.definitions.response;

import javax.xml.bind.annotation.XmlRootElement;

import eu.europeana.cloud.definitions.StatusCode;

@XmlRootElement
public class GenericCloudResponse<T> {
	
	StatusCode statusCode;
	T response;
	public StatusCode getStatusCode(){
		return this.statusCode;
	}
	public void setStatusCode(StatusCode statusCode){
		this.statusCode = statusCode;
	}
	public T getResponse(){
		return response;
	}
	public void setResponse(T response){
		this.response = response;
	}
}
