package eu.europeana.cloud.definitions.response;

public class Response<T> {
	
	int statusCode;
	T response;
	public int getStatusCode(){
		return this.statusCode;
	}
	public void setStatusCode(int statusCode){
		this.statusCode = statusCode;
	}
	public T getResponse(){
		return response;
	}
	public void setResponse(T response){
		this.response = response;
	}
}
