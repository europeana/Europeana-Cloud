package eu.europeana.cloud.service.dps.rest.exceptions;

public class TaskSubmissionException extends Exception{
    
    public TaskSubmissionException(String message){
        super(message);
    }

    public TaskSubmissionException(String message,Throwable e ){
        super(message,e);
    }
}
