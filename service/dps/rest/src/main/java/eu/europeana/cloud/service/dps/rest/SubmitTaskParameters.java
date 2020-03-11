package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.service.dps.DpsTask;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.web.context.request.async.DeferredResult;

import java.net.URI;
import java.util.Date;


@Builder
@Getter
@Setter
@ToString
public class SubmitTaskParameters {
    private DpsTask task;
    private String topologyName;
    private String authorizationHeader;
    private boolean restart;
    private Date sentTime;
    private String taskJSON;
    private URI responsURI;

    private DeferredResult deferredResult;
}
