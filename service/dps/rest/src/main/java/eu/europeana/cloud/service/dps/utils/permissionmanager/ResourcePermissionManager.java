package eu.europeana.cloud.service.dps.utils.permissionmanager;

import eu.europeana.cloud.common.model.Permission;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.rest.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.util.List;

/**
 * Created by Tarek on 4/6/2016.
 */
public abstract class ResourcePermissionManager {
    protected ApplicationContext context;

    protected ResourcePermissionManager(ApplicationContext context) {
        this.context = context;
    }

    /**
     * Grants permissions to all the resources inside the task.
     *
     * @return The number of records inside the task which the permission is given to their version.
     */
    public abstract int grantPermissionsToTaskResources(DpsTask task, String topologyName, String topologyUserName, String authorizationHeader) throws TaskSubmissionException;

    /**
     * Grants permissions to the current user for the specified version.
     */
    protected void grantPermissionToVersion(String authorizationHeader, String topologyUserName, String cloudId, String representationName, String version) throws MCSException, TaskSubmissionException {
        RecordServiceClient recordServiceClient = context.getBean(RecordServiceClient.class);
        recordServiceClient
                .useAuthorizationHeader(authorizationHeader)
                .grantPermissionsToVersion(
                        cloudId,
                        representationName,
                        version, topologyUserName,
                        Permission.ALL);
    }
}
