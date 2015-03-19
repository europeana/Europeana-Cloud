package eu.europeana.cloud.service.dps.rest;

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
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.TaskExecutionReportService;
import eu.europeana.cloud.service.dps.TaskExecutionSubmitService;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;

/**
 * Resource to fetch / submit Tasks to the DPS service
 * 
 */
@Path("/topologies/{topologyName}/tasks")
@Component
public class TasksResource {

	@Autowired
	private TaskExecutionReportService reportService;
	
	@Autowired
	private TaskExecutionSubmitService submitService;

	@Autowired
	private MutableAclService mutableAclService;

	private final static String TOPOLOGY_PREFIX = "Topology";

	/**
	 * Submits a Task
	 */
	@POST
	@Consumes({ MediaType.APPLICATION_JSON })
	@PreAuthorize("hasPermission(#topology," + TOPOLOGY_PREFIX + ", write)")
	public Response submitTask(DpsTask task, @PathParam("topology") String topology) {

		if (task != null) {
			submitService.submitTask(task, topology);
			return Response.ok().build();
		}
		return Response.notModified().build();
	}


	/**
	 * Submits a Task
	 */
	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	public DpsTask getTask(@PathParam("topologyName") String topology) {

		DpsTask task = submitService.fetchTask(topology);
		return task;
	}

	@GET
	@Path("{taskId}/progress")
	public Response getTaskProgress(@PathParam("taskId") String taskId) throws AccessDeniedOrObjectDoesNotExistException {

		String progress = reportService.getTaskProgress(taskId);
    	return Response.ok(progress).build();
	}

	@GET
	@Path("{taskId}/notification")
	public String getTaskNotification(@PathParam("taskId") String taskId) {

		String progress = reportService.getTaskNotification(taskId);
		return progress;
	}
}
