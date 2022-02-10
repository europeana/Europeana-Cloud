package eu.europeana.cloud.service.dps.storm.notification.handler;

import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.storm.notification.NotificationCacheEntry;
import lombok.Builder;
import lombok.Getter;

import java.util.Optional;

/**
 * Configures {@link NotificationTupleHandler}
 */
@Getter
@Builder
public class NotificationHandlerConfig {
    private TaskState taskStateToBeSet;
    /**
     * {@link RecordState} that should be inserted into <b>notifications</b> table.
     */
    private RecordState recordStateToBeSet;
    /**
     * Holds information taken from cache
     */
    private NotificationCacheEntry notificationCacheEntry;

    /**
     * Indicates what task status should be set while processing {@link eu.europeana.cloud.service.dps.storm.NotificationTuple}
     * in {@link eu.europeana.cloud.service.dps.storm.NotificationBolt}</br>
     * Value is wrapped into to {@link Optional} since it may be empty what means that task status should not be changed
     * (processed tuple is not the last one tuple in the task)
     * @return
     */
    public Optional<TaskState> getTaskStateToBeSet() {
        return Optional.ofNullable(taskStateToBeSet);
    }
}
