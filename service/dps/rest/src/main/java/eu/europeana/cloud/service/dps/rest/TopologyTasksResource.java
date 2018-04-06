package eu.europeana.cloud.service.dps.rest;

import com.qmino.miredot.annotations.ReturnType;
import eu.europeana.cloud.common.model.dps.*;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.*;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
import eu.europeana.cloud.service.dps.rest.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.service.utils.TopologyManager;
import eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidator;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.utils.DpsTaskValidatorFactory;
import eu.europeana.cloud.service.dps.utils.PermissionManager;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounter;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import javax.validation.constraints.Min;
import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.List;

import static eu.europeana.cloud.service.dps.InputDataType.*;

/**
 * Resource to fetch / submit Tasks to the DPS service
 */
@Path("/{topologyName}/tasks")
@Component
@Scope("request")
public class TopologyTasksResource {

    @Value("${maxIdentifiersCount}")
    private int maxIdentifiersCount;

    @Autowired
    ApplicationContext context;

    @Autowired
    private TaskExecutionReportService reportService;

    @Autowired
    private ValidationStatisticsReportService validationStatisticsService;

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

    @Autowired
    private FilesCounterFactory filesCounterFactory;


    private final static String TOPOLOGY_PREFIX = "Topology";

    public final static String TASK_PREFIX = "DPS_Task";

    public final static String HTTP_TOPOLOGY = "http_topology";

    private static final int UNKNOWN_EXPECTED_SIZE = -1;

    private static final Logger LOGGER = LoggerFactory.getLogger(TopologyTasksResource.class);

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
     */
    @GET
    @Path("{taskId}/progress")
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', read)")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public TaskInfo getTaskProgress(
            @PathParam("topologyName") String topologyName,
            @PathParam("taskId") String taskId) throws AccessDeniedOrObjectDoesNotExistException, AccessDeniedOrTopologyDoesNotExistException {
        assertContainTopology(topologyName);
        reportService.checkIfTaskExists(taskId, topologyName);
        TaskInfo progress = reportService.getTaskProgress(taskId);
        return progress;
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
    public Response submitTask(@Suspended final AsyncResponse asyncResponse,
                               final DpsTask task,
                               @PathParam("topologyName") final String topologyName,
                               @Context final UriInfo uriInfo,
                               @HeaderParam("Authorization") final String authorizationHeader
    ) throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException {
        if (task != null) {
            LOGGER.info("Submitting task");
            assertContainTopology(topologyName);
            validateTask(task, topologyName);
            final Date sentTime = new Date();
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        String createdTaskUrl = buildTaskUrl(uriInfo, task, topologyName);
                        Response response = Response.created(new URI(createdTaskUrl)).build();
                        taskDAO.insert(task.getTaskId(), topologyName, 0, TaskState.PENDING.toString(), "The task is in a pending mode, it is being processed before submission", sentTime);
                        permissionManager.grantPermissionsForTask(String.valueOf(task.getTaskId()));
                        asyncResponse.resume(response);
                        LOGGER.info("The task is in a pending mode");
                        int expectedSize = getFilesCountInsideTask(task, topologyName, authorizationHeader);
                        if (expectedSize == 0)
                            taskDAO.insert(task.getTaskId(), topologyName, 0, TaskState.DROPPED.toString(), "The task doesn't include any records", sentTime);
                        else {
                            task.addParameter(PluginParameterKeys.AUTHORIZATION_HEADER, authorizationHeader);
                            submitService.submitTask(task, topologyName);
                            LOGGER.info("Task submitted successfully");
                            taskDAO.insert(task.getTaskId(), topologyName, expectedSize, TaskState.SENT.toString(), "", sentTime);
                        }
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
            }).start();

        }
        return Response.notModified().build();
    }

    /**
     * Retrieves a detailed report for the specified task.It will return info about
     * the first 100 resources unless you specified the needed chunk by using from&to parameters
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
     * @param taskId       <strong>REQUIRED</strong> Unique id that identifies the task.
     * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
     * @param from         The starting resource number should be bigger than 0
     * @param to           The ending resource number should be bigger than 0
     * @return Notification messages for the specified task.
     * @summary Retrieve task detailed report
     */
    @GET
    @Path("{taskId}/reports/details")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', read)")
    public List<SubTaskInfo> getTaskDetailedReport(@PathParam("taskId") String taskId, @PathParam("topologyName") final String topologyName, @Min(1) @DefaultValue("1") @QueryParam("from") int from, @Min(1) @DefaultValue("100") @QueryParam("to") int to) throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
        assertContainTopology(topologyName);
        reportService.checkIfTaskExists(taskId, topologyName);
        List<SubTaskInfo> taskInfo = reportService.getDetailedTaskReportBetweenChunks(taskId, from, to);
        return taskInfo;
    }


    /**
     * If error param is not specified it retrieves a report of all errors that occurred for the specified task. For each error
     * the number of occurrences is returned otherwise retrieves a report for a specific error that occurred in the specified task.
     * A sample of identifiers is returned as well. The number of identifiers is between 0 and ${maxIdentifiersCount}.
     * <p>
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
     * @param taskId       <strong>REQUIRED</strong> Unique id that identifies the task.
     * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
     * @param error        Error type.
     * @param idsCount     number of identifiers to retrieve
     * @return Errors that occurred for the specified task.
     * @summary Retrieve task detailed error report
     */
    @GET
    @Path("{taskId}/reports/errors")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', read)")
    public TaskErrorsInfo getTaskErrorReport(@PathParam("taskId") String taskId, @PathParam("topologyName") final String topologyName, @QueryParam("error") String error, @DefaultValue("0") @QueryParam("idsCount") int idsCount) throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
        assertContainTopology(topologyName);
        reportService.checkIfTaskExists(taskId, topologyName);

        if (idsCount < 0 || idsCount > maxIdentifiersCount) {
            throw new IllegalArgumentException("Identifiers count parameter should be between 0 and " + maxIdentifiersCount);
        }
        if (error == null) {
            return reportService.getGeneralTaskErrorReport(taskId, idsCount);
        }
        return reportService.getSpecificTaskErrorReport(taskId, error, idsCount > 0 ? idsCount : maxIdentifiersCount);
    }


    /**
     * Retrieves a statistics report for the specified task. Only applicable for tasks executing {@link eu.europeana.cloud.service.dps.storm.topologies.validation.topology.ValidationTopology}
     * <p>
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
     * @param taskId       <strong>REQUIRED</strong> Unique id that identifies the task.
     * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
     * @return Statistics report for the specified task.
     * @summary Retrieve task statistics report
     */
    @GET
    @Path("{taskId}/statistics")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', read)")
    public StatisticsReport getTaskStatisticsReport(@PathParam("topologyName") String topologyName, @PathParam("taskId") String taskId) throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
        assertContainTopology(topologyName);
        reportService.checkIfTaskExists(taskId, topologyName);
        return validationStatisticsService.getTaskStatisticsReport(Long.valueOf(taskId));
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
     * @throws eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException   if taskId does not belong to the specified topology
     * @summary Kill task
     */

    @POST
    @Path("{taskId}/kill")
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', write)")
    public Response killTask(@PathParam("topologyName") String topologyName, @PathParam("taskId") String taskId) throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
        assertContainTopology(topologyName);
        reportService.checkIfTaskExists(taskId, topologyName);
        killService.killTask(Long.valueOf(taskId));
        return Response.ok("Task killing request was registered successfully").build();

    }

    private String buildTaskUrl(UriInfo uriInfo, DpsTask task, String topologyName) {

        StringBuilder taskUrl = new StringBuilder()
                .append(uriInfo.getBaseUri().toString())
                .append(topologyName)
                .append("/tasks/")
                .append(task.getTaskId());

        return taskUrl.toString();
    }

    private void assertContainTopology(String topology) throws AccessDeniedOrTopologyDoesNotExistException {
        if (!topologyManager.containsTopology(topology)) {
            throw new AccessDeniedOrTopologyDoesNotExistException("The topology doesn't exist");
        }
    }

    private void validateTask(DpsTask task, String topologyName) throws DpsTaskValidationException {

        String taskType = specifyTaskType(task, topologyName);
        DpsTaskValidator validator = DpsTaskValidatorFactory.createValidator(taskType);
        validator.validate(task);
    }

    private String specifyTaskType(DpsTask task, String topologyName) throws DpsTaskValidationException {
        if (task.getDataEntry(FILE_URLS) != null) {
            return topologyName + "_" + FILE_URLS.name().toLowerCase();
        }
        if (task.getDataEntry(DATASET_URLS) != null) {
            return topologyName + "_" + DATASET_URLS.name().toLowerCase();
        }
        if (task.getDataEntry(REPOSITORY_URLS) != null) {
            return topologyName + "_" + REPOSITORY_URLS.name().toLowerCase();
        }
        throw new DpsTaskValidationException("Validation failed. Missing required data_entry");
    }

    /**
     * @return The number of files inside the task.
     */
    private int getFilesCountInsideTask(DpsTask submittedTask, String topologyName, String authorizationHeader) throws TaskSubmissionException {
        if (topologyName.equals(HTTP_TOPOLOGY))
            return UNKNOWN_EXPECTED_SIZE;
        String taskType = getTaskType(submittedTask);
        FilesCounter filesCounter = filesCounterFactory.createFilesCounter(taskType);
        return filesCounter.getFilesCount(submittedTask, authorizationHeader);
    }

    //get TaskType
    private String getTaskType(DpsTask task) {
        //TODO sholud be done in more error prone way
        final InputDataType first = task.getInputData().keySet().iterator().next();
        return first.name();
    }


}