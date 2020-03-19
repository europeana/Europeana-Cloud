package eu.europeana.cloud.service.dps.rest.struct;

import eu.europeana.cloud.service.dps.DpsTask;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.springframework.http.ResponseEntity;
import org.springframework.web.context.request.async.DeferredResult;

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

    /** Start time of processing task */
    private Date sentTime;

    /** JSON representation of Task from request - for future use*/
    private String taskJSON;

    /** Reposn URL/URI - in practis it should be request URL with taskId added at the end of requests */
    private URI responsURI;

    /** Result holder for asynchronous call. It allow to set result (respons) one time and this respons will be returned from call */
    private CompletableFuture<ResponseEntity> responseFuture;
}
