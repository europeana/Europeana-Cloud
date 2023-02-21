package eu.europeana.cloud.common.model.dps;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Describes fields in <b>'error_notifications'</b> table
 */
@Builder
@Getter
@Setter
@ToString
public class ErrorNotification {

  private long taskId;
  private String errorType;
  private String errorMessage;
  private String resource;
  private String additionalInformations;
}
