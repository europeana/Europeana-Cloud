package eu.europeana.cloud.common.model.dps;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@Getter
@AllArgsConstructor
@ToString
public class TaskStateInfo {

    private String topologyName;

    private String state;
}
