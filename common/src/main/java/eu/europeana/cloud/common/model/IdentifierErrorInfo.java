package eu.europeana.cloud.common.model;

import eu.europeana.cloud.common.response.ErrorInfo;
import java.io.Serializable;
import javax.ws.rs.core.Response.Status;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * ErrorInfo wrapper with HTTP code information
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class IdentifierErrorInfo implements Serializable {

  private static final long serialVersionUID = 4623626261467887110L;

  private Status httpCode;

  private ErrorInfo errorInfo;
}
