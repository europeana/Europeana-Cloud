package eu.europeana.cloud.common.model.dps;

import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlRootElement;
import lombok.Builder;
import lombok.EqualsAndHashCode;

@XmlRootElement()
@Builder
@EqualsAndHashCode
public class TaskErrorInfo {

  private String errorType;

  private String message;

  private int occurrences;

  private List<ErrorDetails> errorDetails;


  public TaskErrorInfo() {

  }

  public TaskErrorInfo(String errorType, String message, int occurrences) {
    this(errorType, message, occurrences, null);
  }

  public TaskErrorInfo(String errorType, String message, int occurrences, List<ErrorDetails> errorDetails) {
    this.errorType = errorType;
    this.message = message;
    this.occurrences = occurrences;
    if (errorDetails == null) {
      this.errorDetails = new ArrayList<>(0);
    } else {
      this.errorDetails = errorDetails;
    }
  }


  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public int getOccurrences() {
    return occurrences;
  }

  public void setOccurrences(int occurrences) {
    this.occurrences = occurrences;
  }

  public List<ErrorDetails> getErrorDetails() {
    return errorDetails;
  }

  public void setErrorDetails(List<ErrorDetails> errorDetails) {
    this.errorDetails = errorDetails;
  }

  public String getErrorType() {
    return errorType;
  }

  public void setErrorType(String errorType) {
    this.errorType = errorType;
  }

}
