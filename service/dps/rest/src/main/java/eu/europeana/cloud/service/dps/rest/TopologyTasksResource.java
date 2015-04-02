package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.service.aas.authentication.SpringUserUtils;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.TaskExecutionReportService;
import eu.europeana.cloud.service.dps.TaskExecutionSubmitService;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.stereotype.Component;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

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

    /**
     * Submits a Task
     */
    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    @PreAuthorize("hasPermission(#topology,'" + TOPOLOGY_PREFIX + "', write)")
    public Response submitTask(DpsTask task, @PathParam("topologyName") String topology) {

        if (task != null) {
            submitService.submitTask(task, topology);
            assignPermissionsToTask(task);
            return Response.ok().build();
        }
        return Response.notModified().build();
    }

    /**
     * Submits a Task
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    public DpsTask getTask(@PathParam("topologyName") String topology) {

        DpsTask task = submitService.fetchTask(topology);
        return task;
    }

    @GET
    @Path("{taskId}/progress")
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', read)")
    public Response getTaskProgress(@PathParam("taskId") String taskId) throws AccessDeniedOrObjectDoesNotExistException {

        String progress = reportService.getTaskProgress(taskId);
        return Response.ok(progress).build();
    }

    @GET
    @Path("{taskId}/notification")
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', read)")
    public String getTaskNotification(@PathParam("taskId") String taskId) {

        String progress = reportService.getTaskNotification(taskId);
        return progress;
    }

    private void assignPermissionsToTask(DpsTask submittedTask) {
        ObjectIdentity topologyIdentity = new ObjectIdentityImpl(TASK_PREFIX, submittedTask.getTaskId() + "");

        MutableAcl topologyAcl = mutableAclService.createAcl(topologyIdentity);

        topologyAcl.insertAce(0, BasePermission.WRITE, new PrincipalSid(SpringUserUtils.getUsername()), true);
        topologyAcl.insertAce(0, BasePermission.READ, new PrincipalSid(SpringUserUtils.getUsername()), true);
        mutableAclService.updateAcl(topologyAcl);
    }
}
