package eu.europeana.cloud.tools.statistics;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.LocalDateTime;

@Setter
@Getter
@ToString
@Builder
public class Line {
    LocalDateTime dateTime;
    long taskId;
    boolean startLine;
    String opName;
    String opId;

    public String getKey() {
        return String.format("%d-%s-%s", taskId, opName, opId);
    }
}
