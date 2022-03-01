package eu.europeana.cloud.service.dps.storm;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@Builder
@ToString
public class ErrorType {

    private long taskId;
    private String uuid;
    private String message;
    private int count;

    public void incrementCounter(){
        this.count ++;
    }

}
