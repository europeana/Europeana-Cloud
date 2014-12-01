package eu.europeana.cloud.service.dps.rest;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.service.dps.DpsTask;

/**
 * Resource to manage data sets.
 */
@Path("/tasks")
@Component
@Scope("request")
public class DpsResource {
	
    @Autowired
    // TODO: not currently used. 
    // This is only needed if DPS tasks are assigned permissions
    //private MutableAclService mutableAclService;

    /**
     * Submits a Task
     */
    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    public Response submitTask(DpsTask task)  {
    	
    	return Response.ok().build();
    }
}
