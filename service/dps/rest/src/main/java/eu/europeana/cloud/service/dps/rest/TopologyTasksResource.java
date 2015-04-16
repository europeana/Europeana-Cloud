package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.service.aas.authentication.SpringUserUtils;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.TaskExecutionReportService;
import eu.europeana.cloud.service.dps.TaskExecutionSubmitService;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.NotFoundException;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.stereotype.Component;

import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Resource to fetch / submit Tasks to the DPS service
 */
@Path("/topologies/{topologyName}/tasks")
@Component
public class TopologyTasksResource {

    @Autowired
    private TaskExecutionReportService reportService;

    @Autowired
    private TaskExecutionSubmitService submitService;

    @Autowired
    private MutableAclService mutableAclService;

    private final static String TOPOLOGY_PREFIX = "Topology";
    private final static String TASK_PREFIX = "DPS_Task";

    private static final Logger LOGGER = LoggerFactory.getLogger(TopologyTasksResource.class);

    /**
     * Submits a Task. To call it one has to have write permissions to requested
     * topology.
     *
     * @param task task to submit
     * @param topologyName topology the task is to be submitted
     * @return request response
     */
    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    @PreAuthorize("hasPermission(#topologyName,'" + TOPOLOGY_PREFIX + "', write)")
    public Response submitTask(
            DpsTask task,
            @PathParam("topologyName") String topologyName,
            @Context UriInfo uriInfo) {

        LOGGER.info("Submiting task");
        
        if (task != null) {
            submitService.submitTask(task, topologyName);
            grantPermissionsForTask(task.getTaskId() + "");
            String createdTaskUrl = buildTaskUrl(uriInfo, task, topologyName);
            try {
                LOGGER.info("Task submitted succesfully");
                return Response.created(new URI(createdTaskUrl)).build();
            } catch (URISyntaxException e) {
                LOGGER.error("Task submition failed");
                e.printStackTrace();
                return Response.serverError().build();
            }
        }
        return Response.notModified().build();
    }

    /**
     * Retrieves a Task. Can be called only with admin privileges.
     * @param topologyName topology name
     * @return requested task
     */
    @GET
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', read)")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{taskId}")
    public DpsTask getTask(
            @PathParam("topologyName") String topologyName,
            @PathParam("taskId") String taskId) {

        LOGGER.info("Fetching task");
        DpsTask task = submitService.fetchTask(topologyName, Long.valueOf(taskId));
        return task;
    }

    /**
     * Retrieves progress of the requested task. To call it one has to have read
     * permissions for requested topology.
     *
     * @param taskId task identifier.
     * @return progress for the requested task.
     * @throws
     * eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException
     * if task does not exist or access to the task is denied for the user.
     */
    @GET
    @Path("{taskId}/progress")
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', read)")
    public Response getTaskProgress(
            @PathParam("topologyName") String topologyName,
            @PathParam("taskId") String taskId) throws AccessDeniedOrObjectDoesNotExistException {

        String progress = reportService.getTaskProgress(taskId);
        return Response.ok(progress).build();
    }

    /**
     * Retrieves info messages for the specified task. To call it one has to
     * have read permissions for requested topology.
     *
     * @param taskId task identifier.
     * @return info messages for the specified task
     */
    @GET
    @Path("{taskId}/notification")
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', read)")
    public String getTaskNotification(@PathParam("taskId") String taskId) {

        String progress = reportService.getTaskNotification(taskId);
        return progress;
    }
    
    @POST
    @Path("{taskId}/permit")
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    public Response grantPermissions(@PathParam("topologyName") String topology, @PathParam("taskId") String taskId,
    		@FormParam("username") String username) {

        if (taskId != null && topology  != null) {
        	grantPermissionsForTask(taskId, username);
            return Response.ok().build();
        }
        return Response.notModified().build();
    }

    /**
     * Grants permissions to the current user for the specified task.
     */
    private void grantPermissionsForTask(String taskId) {
    	
    	grantPermissionsForTask(taskId, SpringUserUtils.getUsername());
    }

    /**
     * Grants permissions for the specified task to the specified user.
     */
    private void grantPermissionsForTask(String taskId, String username) {
        
        MutableAcl taskAcl = null;
        ObjectIdentity taskObjectIdentity = new ObjectIdentityImpl(TASK_PREFIX, taskId);
        
        try{
            taskAcl = (MutableAcl) mutableAclService.readAclById(taskObjectIdentity);
        }catch(NotFoundException e){
            taskAcl = mutableAclService.createAcl(taskObjectIdentity);
        }
        Object obj = taskAcl.getEntries();
        taskAcl.insertAce(taskAcl.getEntries().size(), BasePermission.WRITE, new PrincipalSid(username), true);
        taskAcl.insertAce(taskAcl.getEntries().size(), BasePermission.READ, new PrincipalSid(username), true);
        
        mutableAclService.updateAcl(taskAcl);
        
    }

    private String buildTaskUrl(UriInfo uriInfo, DpsTask task, String topologyName) {
        
        StringBuilder taskUrl = new StringBuilder()
                .append(uriInfo.getBaseUri().toString())
                .append("topologies/")
                .append(topologyName)
                .append("/tasks/")
                .append(task.getTaskId());

        return taskUrl.toString();
    }
}
