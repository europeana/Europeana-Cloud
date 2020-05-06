package eu.europeana.cloud.service.dps.storm.spouts.kafka;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsTask;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Date;

/**
 * Set of parameters to submit task to
 */
@Builder
@Getter
@Setter
@ToString
public class SubmitTaskParameters {

    private Date sentTime;

    private int expectedSize=0;

    private TaskState status;

    private String info;

    /** Submitting task */
    private DpsTask task;

    /** Name of processing topology */
    private String topologyName;

    /** Name of processing topic selected for task */
    private String topicName;

    /** Flag if task is subimtted <code>false<code/> or restarted <code>true<code/> */
    private final boolean restart;

    private String taskJSON;
}
