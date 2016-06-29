package eu.europeana.cloud.service.dps.rest;

import com.qmino.miredot.annotations.ReturnType;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.*;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException;
import eu.europeana.cloud.service.dps.rest.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.service.utils.TopologyManager;
import eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidationException;
import eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidator;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.utils.DpsTaskValidatorFactory;
import eu.europeana.cloud.service.dps.utils.PermissionManager;
import eu.europeana.cloud.service.dps.utils.permissionmanager.FilesCounterFactory;
import eu.europeana.cloud.service.dps.utils.permissionmanager.FilesCounter;
import org.glassfish.jersey.server.ManagedAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Resource to fetch / submit Tasks to the DPS service
 */
@Path("/topologies/{topologyName}/tasks")
@Component
public class TopologyTasksResource {

    @Autowired
    ApplicationContext context;

    @Autowired
    private TaskExecutionReportService reportService;

    @Autowired
    private TaskExecutionSubmitService submitService;

    @Autowired
    private TaskExecutionKillService killService;

    @Autowired
    private TopologyManager topologyManager;

    @Autowired
    private PermissionManager permissionManager;

    @Autowired
    private String mcsLocation;

    @Autowired
    private RecordServiceClient recordServiceClient;

    @Autowired
    private FileServiceClient fileServiceClient;

    @Autowired
    private DataSetServiceClient dataSetServiceClient;

    @Autowired
    private CassandraTaskInfoDAO taskDAO;


    private final static String TOPOLOGY_PREFIX = "Topology";
    public final static String TASK_PREFIX = "DPS_Task";

    private static final Logger LOGGER = LoggerFactory.getLogger(TopologyTasksResource.class);

    /**
     * Retrieves a task with the given taskId from the specified topology.
     * <p/>
     * <br/><br/>
     * <div style='border-left: solid 5px #999999; border-radius: 10px; padding: 6px;'>
     * <strong>Required permissions:</strong>
     * <ul>
     * <li>Authenticated user</li>
     * <li>Read permission for selected task</li>
     * </ul>
     * </div>
     *
     * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
     * @param taskId       <strong>REQUIRED</strong> Unique id that identifies the task.
     * @return The requested task.
     * @throws eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException if topology does not exist or access to the topology is denied for the user
     * @summary Task retrieval
     * @summary Task retrieval
     */
    @GET
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', read)")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/{taskId}")
    public DpsTask getTask(
            @PathParam("topologyName") String topologyName,
            @PathParam("taskId") String taskId) throws AccessDeniedOrTopologyDoesNotExistException {

        assertContainTopology(topologyName);

        LOGGER.info("Fetching task");
        DpsTask task = submitService.fetchTask(topologyName, Long.valueOf(taskId));
        return task;
    }

    /**
     * Retrieves the current progress for the requested task.
     * <p/>
     * <br/><br/>
     * <div style='border-left: solid 5px #999999; border-radius: 10px; padding: 6px;'>
     * <strong>Required permissions:</strong>
     * <ul>
     * <li>Read permissions for selected task</li>
     * </ul>
     * </div>
     *
     * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
     * @param taskId       <strong>REQUIRED</strong> Unique id that identifies the task.
     * @return Progress for the requested task
     * (number of records of the specified task that have been fully processed).
     * @throws eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException   if task does not exist or access to the task is denied for the user
     * @throws eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException if topology does not exist or access to the topology is denied for the user
     * @summary Get Task Progress
     * @summary Get Task Progress
     */
    @GET
    @Path("{taskId}/progress")
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', read)")
    @ReturnType("java.lang.String")
    public Response getTaskProgress(
            @PathParam("topologyName") String topologyName,
            @PathParam("taskId") String taskId) throws AccessDeniedOrObjectDoesNotExistException, AccessDeniedOrTopologyDoesNotExistException {

        assertContainTopology(topologyName);

        String progress = reportService.getTaskProgress(taskId);
        return Response.ok(progress).build();
    }

    /**
     * Submits a Task for execution.
     * Each Task execution is associated with a specific plugin.
     * <p/>
     * <strong>Write permissions required</strong>.
     *
     * @param task         <strong>REQUIRED</strong> Task to be executed. Should contain links to input data,
     *                     either in form of cloud-records or cloud-datasets.
     * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
     * @return URI with information about the submitted task execution.
     * @throws eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException if topology does not exist or access to the topology is denied for the user
     * @summary Submit Task
     * @summary Submit Task
     */
    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    @PreAuthorize("hasPermission(#topologyName,'" + TOPOLOGY_PREFIX + "', write)")
    @ManagedAsync
    @Path("/")
    public Response submitTask(@Suspended final AsyncResponse asyncResponse,
                               DpsTask task,
                               @PathParam("topologyName") String topologyName,
                               @Context UriInfo uriInfo,
                               @HeaderParam("Authorization") String authorizationHeader
    ) throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException {
        if (task != null) {
            LOGGER.info("Submitting task");
            assertContainTopology(topologyName);
            validateTask(task, topologyName);
            Date sentTime = new Date();
            try {
                String createdTaskUrl = buildTaskUrl(uriInfo, task, topologyName);
                Response response = Response.created(new URI(createdTaskUrl)).build();
                taskDAO.insert(task.getTaskId(), topologyName, 0, TaskState.PENDING.toString(), "The task is in a pending mode, it is being processed before submission", sentTime);
                asyncResponse.resume(response);
                LOGGER.info("The task is in a pending mode");
                int expectedSize = getFilesCountInsideTask(task, authorizationHeader);
                task.addParameter(PluginParameterKeys.AUTHORIZATION_HEADER, authorizationHeader);
                submitService.submitTask(task, topologyName);
                permissionManager.grantPermissionsForTask(String.valueOf(task.getTaskId()));
                LOGGER.info("Task submitted successfully");
                taskDAO.insert(task.getTaskId(), topologyName, expectedSize, TaskState.SENT.toString(), "", sentTime);
            } catch (URISyntaxException e) {
                LOGGER.error("Task submission failed");
                e.printStackTrace();
                Response response = Response.serverError().build();
                taskDAO.insert(task.getTaskId(), topologyName, 0, TaskState.DROPPED.toString(), e.getMessage(), sentTime);
                asyncResponse.resume(response);
            } catch (TaskSubmissionException e) {
                LOGGER.error("Task submission failed" + e.getMessage());
                taskDAO.insert(task.getTaskId(), topologyName, 0, TaskState.DROPPED.toString(), e.getMessage(), sentTime);
                e.printStackTrace();
            } catch (Exception e) {
                LOGGER.error("Task submission failed." + e.getMessage());
                taskDAO.insert(task.getTaskId(), topologyName, 0, TaskState.DROPPED.toString(), e.getMessage(), sentTime);
                e.printStackTrace();
                Response response = Response.serverError().build();
                asyncResponse.resume(response);
            }
        }
        return Response.notModified().build();
    }


    /**
     * Retrieves notifications for the specified task.
     * <p/>
     * <br/><br/>
     * <div style='border-left: solid 5px #999999; border-radius: 10px; padding: 6px;'>
     * <strong>Required permissions:</strong>
     * <ul>
     * <li>Authenticated user</li>
     * <li>Read permission for selected task</li>
     * </ul>
     * </div>
     *
     * @param taskId <strong>REQUIRED</strong> Unique id that identifies the task.
     * @return Notification messages for the specified task.
     * @summary Retrieve task notifications
     */
    @GET
    @Path("{taskId}/notification")
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', read)")
    public String getTaskNotification(@PathParam("taskId") String taskId) {

        String progress = reportService.getTaskNotification(taskId);
        return progress;
    }

    /**
     * Grants read / write permissions for a task to the specified user.
     * <p/>
     * <br/><br/>
     * <div style='border-left: solid 5px #999999; border-radius: 10px; padding: 6px;'>
     * <strong>Required permissions:</strong>
     * <ul>
     * <li>Admin permissions</li>
     * </ul>
     * </div>
     *
     * @param taskId       <strong>REQUIRED</strong> Unique id that identifies the task.
     * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
     * @param username     <strong>REQUIRED</strong> Permissions are granted to the account with this unique username
     * @return Status code indicating whether the operation was successful or not.
     * @throws eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException if topology does not exist or access to the topology is denied for the user
     * @summary Grant task permissions to user
     * @summary Grant task permissions to user
     */
    @POST
    @Path("{taskId}/permit")
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    @ReturnType("java.lang.Void")
    public Response grantPermissions(@PathParam("topologyName") String topologyName, @PathParam("taskId") String taskId,
                                     @FormParam("username") String username) throws AccessDeniedOrTopologyDoesNotExistException {

        assertContainTopology(topologyName);

        if (taskId != null) {
            permissionManager.grantPermissionsForTask(taskId, username);
            return Response.ok().build();
        }
        return Response.notModified().build();
    }

    /**
     * Submit kill flag to the specific task.
     * <p/>
     * Side effect: remove all flags older than 5 days (per topology).
     * <p/>
     * <br/><br/>
     * <div style='border-left: solid 5px #999999; border-radius: 10px; padding: 6px;'>
     * <strong>Required permissions:</strong>
     * <ul>
     * <li>Authenticated user</li>
     * <li>Write permission for selected task</li>
     * </ul>
     * </div>
     *
     * @param taskId       <strong>REQUIRED</strong> Unique id that identifies the task.
     * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
     * @return Status code indicating whether the operation was successful or not.
     * @throws eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException if topology does not exist or access to the topology is denied for the user
     * @summary Kill task
     * @summary Kill task
     */
    @POST
    @Path("{taskId}/kill")
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', write)")
    @ReturnType("java.lang.Void")
    public Response killTask(@PathParam("topologyName") String topologyName, @PathParam("taskId") String taskId) throws AccessDeniedOrTopologyDoesNotExistException {
        assertContainTopology(topologyName);

        if (taskId != null) {
            killService.killTask(topologyName, Long.valueOf(taskId));
            killService.cleanOldFlags(topologyName, TimeUnit.DAYS.toMillis(5)); //side effect
            return Response.ok().build();
        }
        return Response.notModified().build();
    }

    /**
     * Check kill flag for the specified task.
     * <p/>
     * <br/><br/>
     * <div style='border-left: solid 5px #999999; border-radius: 10px; padding: 6px;'>
     * <strong>Required permissions:</strong>
     * <ul>
     * <li>Authenticated user</li>
     * <li>Read permission for selected task</li>
     * </ul>
     * </div>
     *
     * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
     * @param taskId       <strong>REQUIRED</strong> Unique id that identifies the task.
     * @return true if provided task id has kill flag, false otherwise
     * @throws eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException if topology does not exist or access to the topology is denied for the user
     * @summary Check kill flag
     * @summary Check kill flag
     */
    @GET
    @Path("{taskId}/kill")
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', read)")
    public Boolean checkKillFlag(@PathParam("topologyName") String topologyName, @PathParam("taskId") String taskId) throws AccessDeniedOrTopologyDoesNotExistException {
        assertContainTopology(topologyName);

        return killService.hasKillFlag(topologyName, Long.valueOf(taskId));
    }

    /**
     * Remove kill flag for the specified task.
     * <p/>
     * <br/><br/>
     * <div style='border-left: solid 5px #999999; border-radius: 10px; padding: 6px;'>
     * <strong>Required permissions:</strong>
     * <ul>
     * <li>Authenticated user</li>
     * <li>Write permission for selected task</li>
     * </ul>
     * </div>
     *
     * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
     * @param taskId       <strong>REQUIRED</strong> Unique id that identifies the task.
     * @return Status code indicating whether the operation was successful or not.
     * @throws eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException if topology does not exist or access to the topology is denied for the user
     * @summary Remove kill flag
     * @summary Remove kill flag
     */
    @DELETE
    @Path("{taskId}/kill")
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', write)")
    @ReturnType("java.lang.Void")
    public Response removeKillFlag(@PathParam("topologyName") String topologyName, @PathParam("taskId") String taskId) throws AccessDeniedOrTopologyDoesNotExistException {
        assertContainTopology(topologyName);

        if (taskId != null && topologyName != null) {
            killService.removeFlag(topologyName, Long.valueOf(taskId));
            return Response.ok().build();
        }
        return Response.notModified().build();
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

    private void assertContainTopology(String topology) throws AccessDeniedOrTopologyDoesNotExistException {
        if (!topologyManager.containsTopology(topology)) {
            throw new AccessDeniedOrTopologyDoesNotExistException();
        }
    }

    private void validateTask(DpsTask task, String topologyName) throws DpsTaskValidationException {

        String taskType = specifyTaskType(task, topologyName);
        DpsTaskValidator validator = DpsTaskValidatorFactory.createValidator(taskType);
        validator.validate(task);
    }

    private String specifyTaskType(DpsTask task, String topologyName) throws DpsTaskValidationException {
        if (task.getDataEntry(PluginParameterKeys.FILE_URLS) != null) {
            return topologyName + "_" + PluginParameterKeys.FILE_URLS.toLowerCase();
        }
        if (task.getDataEntry(PluginParameterKeys.DATASET_URLS) != null) {
            return topologyName + "_" + PluginParameterKeys.DATASET_URLS.toLowerCase();
        }
        throw new DpsTaskValidationException("Validation failed. Missing required data_entry");
    }

    /**
     * @return The number of records inside the task.
     */
    private int getFilesCountInsideTask(DpsTask submittedTask, String authorizationHeader) throws TaskSubmissionException {
        FilesCounterFactory filesCounterFactory = new FilesCounterFactory(context);
        String taskType = getTaskType(submittedTask);
        FilesCounter filesCounter = filesCounterFactory.createFilesCounter(taskType);
        int recordsInsideTask = filesCounter.getFilesCount(submittedTask, authorizationHeader);
        return recordsInsideTask;
    }

    //get TaskType
    private String getTaskType(DpsTask task) {
        if (task.getInputData().get(DpsTask.FILE_URLS) != null)
            return PluginParameterKeys.FILE_URLS;
        return PluginParameterKeys.DATASET_URLS;

    }


}