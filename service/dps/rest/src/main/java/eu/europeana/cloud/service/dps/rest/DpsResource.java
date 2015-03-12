package eu.europeana.cloud.service.dps.rest;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.springframework.security.acls.model.MutableAclService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.service.dps.DpsService;
import eu.europeana.cloud.service.dps.DpsTask;
import org.springframework.security.access.prepost.PreAuthorize;

/**
 * Resource to fetch / submit Tasks to the DPS service
 * 
 */
@Path("/tasks")
@Component
@Scope("request")
public class DpsResource {

	@Autowired
	private DpsService dps;
	
	@Autowired
        private MutableAclService mutableAclService;

        private final static String TOPOLOGY_PREFIX = "Topology";
              
        
        //Topology.ID
    /**
     * Submits a Task
     */
    @POST
    @Path("{topology}")
    @Consumes({MediaType.APPLICATION_JSON})
    @PreAuthorize("hasPermission(#topology," + TOPOLOGY_PREFIX + ", write)")
//    @PreAuthorize("hasPermission(#globalId.concat('/').concat(#schema).concat('/').concat(#version),"
//    		+ " 'eu.europeana.cloud.common.model.Representation', write)")
    public Response submitTask(DpsTask task, @PathParam("topology") String topology)  {
    	
    	if (task != null) {
    		dps.submitTask(task, topology);
        	return Response.ok().build();
    	}
    	return Response.notModified().build();
    }
    
    
    /**
     * Submits a Task
     */
    @GET
    @Path("{topology}")
    @Produces({MediaType.APPLICATION_JSON})
    public DpsTask getTask(@PathParam("topology") String topology)  {
    	
    	DpsTask task = dps.fetchTask(topology);
    	return task;
    }
    
    @GET
    @Path("{taskId}/progress")
    public String getTaskProgress(@PathParam("taskId") String taskId)  {
    	
    	String progress = dps.getTaskProgress(taskId);
    	return progress;
    }
    
    @GET
    @Path("{taskId}/notification")
    public String getTaskNotification(@PathParam("taskId") String taskId)  {
    	
    	String progress = dps.getTaskNotification(taskId);
    	return progress;
    }
}
