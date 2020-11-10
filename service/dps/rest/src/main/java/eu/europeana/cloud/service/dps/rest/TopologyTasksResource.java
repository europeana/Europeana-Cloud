package eu.europeana.cloud.service.dps.rest;

import com.qmino.miredot.annotations.ReturnType;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.TaskExecutionReportService;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import eu.europeana.cloud.service.dps.services.DatasetCleanerService;
import eu.europeana.cloud.service.dps.services.SubmitTaskService;
import eu.europeana.cloud.service.dps.services.validation.TaskSubmissionValidator;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.utils.PermissionManager;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;

/**
 * Resource to fetch / submit Tasks to the DPS service
 */
@RestController
@RequestMapping("/{topologyName}/tasks")
public class TopologyTasksResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopologyTasksResource.class);

    private static final String TOPOLOGY_PREFIX = "Topology";

    public static final String TASK_PREFIX = "DPS_Task";

    @Autowired
    private TaskExecutionReportService reportService;

    @Autowired
    private PermissionManager permissionManager;

    @Autowired
    private CassandraTaskInfoDAO taskInfoDAO;

    @Autowired
    private TaskStatusUpdater taskStatusUpdater;

    @Autowired
    private DatasetCleanerService datasetCleanerService;

    @Autowired
    private SubmitTaskService submitTaskService;

    @Autowired
    private TaskSubmissionValidator taskSubmissionValidator;

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

    @GetMapping(value = "{taskId}/progress", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    @PreAuthorize("hasPermission(#taskId,'" + TASK_PREFIX + "', read)")
    public TaskInfo getTaskProgress(
            @PathVariable final String topologyName,
            @PathVariable final String taskId) throws
                            AccessDeniedOrObjectDoesNotExistException, AccessDeniedOrTopologyDoesNotExistException {
        taskSubmissionValidator.assertContainTopology(topologyName);
        reportService.checkIfTaskExists(taskId, topologyName);
        return reportService.getTaskProgress(taskId);
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
    @PostMapping(consumes = {MediaType.APPLICATION_JSON_VALUE})
    @PreAuthorize("hasPermission(#topologyName,'" + TOPOLOGY_PREFIX + "', write)")
    public ResponseEntity<Void> submitTask(
            final HttpServletRequest request,
            @RequestBody final DpsTask task,
            @PathVariable final String topologyName,
            @RequestHeader("Authorization") final String authorizationHeader
    ) throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, IOException {
        return doSubmitTask(request, task, topologyName, authorizationHeader, false);
    }

    /**
     * Restarts a Task for execution.
     * Each Task execution is associated with a specific plugin.
     * <p/>
     * <strong>Write permissions required</strong>.
     *
     * @param taskId       <strong>REQUIRED</strong> Task identifier to be processed.
     * @param topologyName <strong>REQUIRED</strong> Name of the topology where the task is submitted.
     * @return URI with information about the submitted task execution.
     * @throws eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException if topology does not exist or access to the topology is denied for the user
     * @summary Submit Task
     * @summary Submit Task
     */
    @PostMapping(path = "{taskId}/restart", consumes = {MediaType.APPLICATION_JSON_VALUE})
    @PreAuthorize("hasPermission(#topologyName,'" + TOPOLOGY_PREFIX + "', write)")
    public ResponseEntity<Void> restartTask(
            final HttpServletRequest request,
            @PathVariable final long taskId,
            @PathVariable final String topologyName,
            @RequestHeader("Authorization") final String authorizationHeader
    ) throws TaskInfoDoesNotExistException, AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, IOException {
        TaskInfo taskInfo = taskInfoDAO.findById(taskId).orElseThrow(TaskInfoDoesNotExistException::new);
        DpsTask task = new ObjectMapper().readValue(taskInfo.getTaskDefinition(), DpsTask.class);
        return doSubmitTask(request, task, topologyName, authorizationHeader, true);
    }

    @PostMapping(path = "{taskId}/cleaner", consumes = {MediaType.APPLICATION_JSON_VALUE})
    @PreAuthorize("hasPermission(#topologyName,'" + TOPOLOGY_PREFIX + "', write)")
    public ResponseEntity<Void> cleanIndexingDataSet(
            @PathVariable final String topologyName,
            @PathVariable final String taskId,
            @RequestBody final DataSetCleanerParameters cleanerParameters
    ) throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
        LOGGER.info("Cleaning parameters for: {}", cleanerParameters);

        taskSubmissionValidator.assertContainTopology(topologyName);
        reportService.checkIfTaskExists(taskId, topologyName);
        datasetCleanerService.clean(taskId, cleanerParameters);
        return ResponseEntity.ok().build();
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

    @PostMapping(path = "{taskId}/permit")
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    @ReturnType("java.lang.Void")
    public ResponseEntity<Void> grantPermissions(
            @PathVariable String topologyName,
            @PathVariable String taskId,
            @RequestParam String username) throws AccessDeniedOrTopologyDoesNotExistException {

        taskSubmissionValidator.assertContainTopology(topologyName);

        if (taskId != null) {
            permissionManager.grantPermissionsForTask(taskId, username);
            return ResponseEntity.ok().build();
        }
        return ResponseEntity.status(HttpStatus.NOT_MODIFIED).build();
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
     * @param info         <strong>OPTIONAL</strong> The cause of the cancellation. If it was not specified a default cause 'Dropped by the user' will be provided
     * @return Status code indicating whether the operation was successful or not.
     * @throws eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException if topology does not exist or access to the topology is denied for the user
     * @throws eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException   if taskId does not belong to the specified topology
     * @summary Kill task
     */

    @PostMapping(path = "{taskId}/kill")
    @PreAuthorize("hasRole('ROLE_ADMIN') OR  hasPermission(#taskId,'" + TASK_PREFIX + "', write)")
    public ResponseEntity<String> killTask(
            @PathVariable String topologyName,
            @PathVariable String taskId,
            @RequestParam(defaultValue = "Dropped by the user") String info)
                    throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
        taskSubmissionValidator.assertContainTopology(topologyName);
        reportService.checkIfTaskExists(taskId, topologyName);
        taskStatusUpdater.setTaskDropped(Long.parseLong(taskId), info);
        return ResponseEntity.ok("The task was killed because of " + info);
    }

    /**
     * Common method for submit/restart task. Mode is given in restart parameter
     * @param task Task to process to
     * @param topologyName Name of processing topology
     * @param authorizationHeader Header for authorisation
     * @param restart Mode (submit = <code>false</code> / restart = <code>true</code>) flag
     * @return Respons for rest call
     * @throws AccessDeniedOrTopologyDoesNotExistException
     * @throws DpsTaskValidationException
     * @throws IOException
     */
    private ResponseEntity<Void> doSubmitTask(
            final HttpServletRequest request,
            final DpsTask task, final String topologyName,
            final String authorizationHeader, final boolean restart)
            throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, IOException {

        ResponseEntity<Void> result;

        if (task != null) {
            LOGGER.info(!restart ? "Submitting task" : "Restarting task");
            task.addParameter(PluginParameterKeys.AUTHORIZATION_HEADER, authorizationHeader);
            String taskJSON = new ObjectMapper().writeValueAsString(task);
            SubmitTaskParameters parameters = SubmitTaskParameters.builder()
                    .sentTime(new Date())
                    .task(task)
                    .topologyName(topologyName)
                    .status(TaskState.PROCESSING_BY_REST_APPLICATION)
                    .info("The task is in a pending mode, it is being processed before submission")
                    .taskJSON(taskJSON)
                    .restarted(restart).build();
            try {
                taskStatusUpdater.insertTask(parameters);
                taskSubmissionValidator.validateTaskSubmission(parameters);
                permissionManager.grantPermissionsForTask(String.valueOf(task.getTaskId()));
                submitTaskService.submitTask(parameters);
                URI responseURI  = buildTaskURI(request.getRequestURL(), task);
                result = ResponseEntity.created(responseURI).build();
            } catch(DpsTaskValidationException | AccessDeniedOrTopologyDoesNotExistException e) {
                throw e;
            } catch(Exception e) {
                result = getResponseForException(e, "Task submission failed. Internal server error.",
                        HttpStatus.INTERNAL_SERVER_ERROR, parameters);
            }
        } else {
            LOGGER.error("Task submission failed. Internal server error. DpsTask task is null.");
            result = ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }

        return result;
    }


    private ResponseEntity<Void> getResponseForException(Exception exception, String loggedMessage, HttpStatus httpStatus,
                                                         SubmitTaskParameters parameters) {
        LOGGER.error(loggedMessage);
        ResponseEntity<Void> response = ResponseEntity.status(httpStatus).build();
        taskStatusUpdater.setTaskDropped(parameters.getTask().getTaskId(), exception.getMessage());
        return response;
    }

    private URI buildTaskURI(StringBuffer base, DpsTask task) throws URISyntaxException {
        if(base.charAt(base.length()-1) != '/') {
            base.append('/');
        }
        base.append(task.getTaskId());
        return new URI(base.toString());
   }
}