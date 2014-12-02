package eu.europeana.cloud.service.dps.rest;

import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.collect.ImmutableList;

import eu.europeana.cloud.service.dps.DpsKeys;
import eu.europeana.cloud.service.dps.DpsTask;

/**
 * Resource to fetch / submit Tasks to the DPS service
 * 
 */
@Path("/tasks")
@Component
@Scope("request")
public class DpsResource {
	
	//    @Autowired
    // TODO: not currently used. 
    // This is only needed if DPS tasks are assigned permissions
    //private MutableAclService mutableAclService;

    /**
     * Submits a Task
     */
    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    public Response submitTask(DpsTask task)  {
    	
    	if (task != null) {
        	return Response.ok().build();
    	}
    	return Response.notModified().build();
    }
    
    /**
     * Submits a Task
     */
    @GET
    @Path("/{type}")
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public DpsTask getTask(@PathParam("type") String providerId)  {
    	
    	DpsTask task = generateDpsTask();
    	if (task != null) {
        	return task;
    	}
    	return null;
    }
    
    /**
	 * @return Dummy Implementation, always returns the same hard-coded Task
	 */
	private static DpsTask generateDpsTask() {

		DpsTask task = new DpsTask();
		
		final String fileUrl = "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/"
				+ "L9WSPSMVQ85/representations/edm/versions/b17c4f60-70d0-11e4-8fe1-00163eefc9c8/files/ef9322a1-5416-4109-a727-2bdfecbf352d";
		
		final String xsltUrl = "http://myxslt.url.com";
		
		task.addDataEntry(DpsTask.FILE_URLS, ImmutableList.of(fileUrl));
		task.addParameter(DpsKeys.XSLT_URL, xsltUrl);
		task.addParameter(DpsKeys.OUTPUT_URL, fileUrl);
		
		return task;
	}
}
