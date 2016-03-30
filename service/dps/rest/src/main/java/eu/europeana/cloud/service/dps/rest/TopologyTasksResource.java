package eu.europeana.cloud.service.dps.rest;

import com.qmino.miredot.annotations.ReturnType;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Permission;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.TaskExecutionKillService;
import eu.europeana.cloud.service.dps.TaskExecutionReportService;
import eu.europeana.cloud.service.dps.TaskExecutionSubmitService;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException;
import eu.europeana.cloud.service.dps.rest.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.service.utils.TopologyManager;
import eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidationException;
import eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidator;
import eu.europeana.cloud.service.dps.utils.DpsTaskValidatorFactory;
import eu.europeana.cloud.service.dps.utils.PermissionManager;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
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
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;
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
    @Path("/")
    public Response submitTask(
            DpsTask task,
            @PathParam("topologyName") String topologyName,
            @Context UriInfo uriInfo,
            @HeaderParam("Authorization") String authorizationHeader
    ) throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException {

        LOGGER.info("Submiting task");

        assertContainTopology(topologyName);
        validateTask(task, topologyName);

        if (task != null) {
            grantPermissionsToTaskResources(topologyName, authorizationHeader, task);

            submitService.submitTask(task, topologyName);
            permissionManager.grantPermissionsForTask(task.getTaskId() + "");
            String createdTaskUrl = buildTaskUrl(uriInfo, task, topologyName);
            try {
                LOGGER.info("Task submitted successfully");
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
        
        String taskType = specifyTaskType(task,topologyName);
        DpsTaskValidator validator = DpsTaskValidatorFactory.createValidator(taskType);
        validator.validate(task);
    }

    private String specifyTaskType(DpsTask task, String topologyName) throws DpsTaskValidationException {
        if(topologyName.equals("xslt_topology"))
            return topologyName;
        if (task.getDataEntry(PluginParameterKeys.FILE_URLS) != null) {
            return topologyName + "_" + PluginParameterKeys.FILE_URLS.toLowerCase();
        }
        if (task.getDataEntry(PluginParameterKeys.DATASET_URLS) != null) {
            return topologyName + "_" + PluginParameterKeys.DATASET_URLS.toLowerCase();
        }
        throw new DpsTaskValidationException("Validation failed. Missing required data_entry");
    }

    private void grantPermissionsToTaskResources(String topologyName, String authorizationHeader, DpsTask submittedTask) throws TaskSubmissionException {

        LOGGER.info("Granting permissions to files from DPS task");
        String topologyUserName = topologyManager.getNameToUserMap().get(topologyName);
        if (topologyUserName == null) {
            LOGGER.error("There is no user for topology '{}' in users map. Permissions will not be granted.", topologyName);
            return;
        }
        int size = 0;
        List<String> fileUrls = submittedTask.getInputData().get(DpsTask.FILE_URLS);
        if (fileUrls == null) {
            List<String> dataSets = submittedTask.getInputData().get(DpsTask.DATASET_URLS);
            if (dataSets == null) {
                LOGGER.info("Datasets or files urls list is empty. Permissions will not be granted");
                throw new TaskSubmissionException("Datasets or files urls list is empty. Permissions will not be granted. Submission process stopped.");
            } else {
                String representationName = submittedTask.getParameter(PluginParameterKeys.REPRESENTATION_NAME);
                for (String dataSet : dataSets) {
                    try {
                        UrlParser urlParser = new UrlParser(dataSet);
                        if (urlParser.isUrlToDataset()) {
                            dataSetServiceClient = context.getBean(DataSetServiceClient.class);
                            List<Representation> representations = dataSetServiceClient.useAuthorizationHeader(authorizationHeader).getDataSetRepresentations(urlParser.getPart(UrlPart.DATA_PROVIDERS),
                                    urlParser.getPart(UrlPart.DATA_SETS));
                            for (Representation representation : representations) {
                                if (representationName == null || representation.getRepresentationName().equals(representationName)) {
                                    List<File> files = representation.getFiles();
                                    for (File file : files) {
                                        fileServiceClient = context.getBean(FileServiceClient.class);
                                        String fileUrl = fileServiceClient.useAuthorizationHeader(authorizationHeader).getFileUri(representation.getCloudId(), representation.getRepresentationName(), representation.getVersion(), file.getFileName()).toString();
                                        grantPermissionToFile(fileUrl, authorizationHeader, topologyUserName);
                                        size++;
                                    }
                                }
                            }
                        }
                    } catch (DataSetNotExistsException ex) {
                        LOGGER.warn("Provided dataset is not existed {}", dataSet);
                        throw new TaskSubmissionException("Provided dataset is not existed: " + dataSet + ". Submission process stopped.");


                    } catch (MalformedURLException ex) {
                        LOGGER.error("URL in task's dataset list is malformed. Submission terminated. Wrong entry: " + dataSet);
                        throw new TaskSubmissionException("Malformed URL in task: " + dataSet + ". Submission process stopped.");

                    } catch (MCSException ex) {
                        LOGGER.error("Error while communicating MCS", ex);
                        throw new TaskSubmissionException("Error while communicating MCS. " + ex.getMessage() + " for: " + dataSet + ". Submission process stopped.");
                    }
                }
            }
        } else {
            Iterator<String> it = fileUrls.iterator();
            while (it.hasNext()) {
                String fileUrl = it.next();
                grantPermissionToFile(fileUrl, authorizationHeader, topologyUserName);
                size++;
            }
        }
        submittedTask.addParameter(PluginParameterKeys.EXPECTED_SIZE, String.valueOf(size));
    }

    private void grantPermissionToFile(String fileUrl, String authorizationHeader, String topologyUserName) throws TaskSubmissionException {
        try {
            UrlParser parser = new UrlParser(fileUrl);
            if (parser.isUrlToRepresentationVersionFile()) {
                recordServiceClient = context.getBean(RecordServiceClient.class);
                recordServiceClient
                        .useAuthorizationHeader(authorizationHeader)
                        .grantPermissionsToVersion(
                                parser.getPart(UrlPart.RECORDS),
                                parser.getPart(UrlPart.REPRESENTATIONS),
                                parser.getPart(UrlPart.VERSIONS),
                                topologyUserName,
                                Permission.ALL);
                LOGGER.info("Permissions granted to: {}", fileUrl);
            } else {
                LOGGER.info("Permissions was not granted. Url does not point to file: {}", fileUrl);
            }
        } catch (MalformedURLException e) {
            LOGGER.error("URL in task's file list is malformed. Submission terminated. Wrong entry: " + fileUrl);
            throw new TaskSubmissionException("Malformed URL in task: " + fileUrl + ". Submission process stopped.");
        } catch (MCSException e) {
            LOGGER.error("Error while communicating MCS", e);
            throw new TaskSubmissionException("Error while communicating MCS. " + e.getMessage() + " for: " + fileUrl + ". Submission process stopped.");
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage() + ". Submission process stopped");
        }
    }
}
