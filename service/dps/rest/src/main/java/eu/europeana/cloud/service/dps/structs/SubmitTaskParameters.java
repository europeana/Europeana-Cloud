package eu.europeana.cloud.service.dps.structs;

import eu.europeana.cloud.service.dps.DpsTask;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.springframework.http.ResponseEntity;

import java.net.URI;
import java.util.Date;
import java.util.concurrent.CompletableFuture;

/**
 * Set of parameters to submit task to
 */
@Builder
@Getter
@ToString
public class SubmitTaskParameters {
    /** Submitting task */
    private DpsTask task;

    /** Name of processing topology */
    private String topologyName;

    /** Authorisation header for web request */
    private String authorizationHeader;

    /** Flag if task is subimtted <code>false<code/> or restarted <code>true<code/> */
    private boolean restart;
}
