package eu.europeana.cloud.service.dps.storm.notification.handler;

import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.service.dps.storm.notification.NotificationCacheEntry;
import lombok.Builder;
import lombok.Getter;

/**
 * Configures {@link NotificationTupleHandler}
 */
@Getter
@Builder
public class NotificationHandlerConfig {
   /**
     * {@link RecordState} that should be inserted into <b>notifications</b> table.
     */
    private RecordState recordStateToBeSet;
    /**
     * Holds information taken from cache
     */
    private NotificationCacheEntry notificationCacheEntry;

}
