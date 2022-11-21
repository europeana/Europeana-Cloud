package eu.europeana.cloud.common.response;

import com.fasterxml.jackson.annotation.JsonRootName;
import lombok.*;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * Rest response that is returned if some error occurred.
 */
@XmlRootElement
@JsonRootName("errorInfo")
@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class ErrorInfo {

  /**
   * Code of error. This is specific for a particular rest api.
   */
  private String errorCode;

  /**
   * Details message for error.
   */
  private String details;
}
