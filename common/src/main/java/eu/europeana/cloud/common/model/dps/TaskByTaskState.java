package eu.europeana.cloud.common.model.dps;

import lombok.*;

import java.util.Date;

@Getter
@Setter
@Builder
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class TaskByTaskState {
    private TaskState state;
    private String topologyName;
    private Long id;
    private String applicationId;
    private Date startTime;
    private String topicName;
}
