package eu.europeana.cloud.common.model.dps;

import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

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
