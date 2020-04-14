package eu.europeana.cloud.service.dps.structs;

import eu.europeana.cloud.service.dps.DpsTask;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 * Set of parameters to submit task to
 */
@Builder
@Getter
@ToString
public class SubmitTaskParameters {
    /** Submitting task */
    private final DpsTask task;

    /** Name of processing topology */
    private final String topologyName;

    /** Authorisation header for web request */
    private final String authorizationHeader;

    /** Flag if task is subimtted <code>false<code/> or restarted <code>true<code/> */
    private final boolean restart;
}
