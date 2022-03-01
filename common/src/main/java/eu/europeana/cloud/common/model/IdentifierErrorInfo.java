package eu.europeana.cloud.common.model;

import eu.europeana.cloud.common.response.ErrorInfo;
import lombok.*;

import javax.ws.rs.core.Response.Status;

/**
 * ErrorInfo wrapper with HTTP code information
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
 public class IdentifierErrorInfo {
	private static final long serialVersionUID = 4623626261467887110l;

	private Status httpCode;
	
	private ErrorInfo errorInfo;
}
