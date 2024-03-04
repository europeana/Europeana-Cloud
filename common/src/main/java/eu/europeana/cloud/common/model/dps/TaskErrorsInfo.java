package eu.europeana.cloud.common.model.dps;

import jakarta.xml.bind.annotation.XmlRootElement;

import java.util.ArrayList;
import java.util.List;

@XmlRootElement
public class TaskErrorsInfo {

  private long id;

  private List<TaskErrorInfo> errors;

  public TaskErrorsInfo() {

  }

  public TaskErrorsInfo(long id) {
    this(id, null);
  }

  public TaskErrorsInfo(long id, List<TaskErrorInfo> errors) {
    this.id = id;
    if (errors == null) {
      this.errors = new ArrayList<>();
    } else {
      this.errors = errors;
    }
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getId() {
    return id;
  }

  public List<TaskErrorInfo> getErrors() {
    return errors;
  }

  public void setErrors(List<TaskErrorInfo> errors) {
    this.errors = errors;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }

    TaskErrorsInfo taskInfo = (TaskErrorsInfo) o;

    if (id != taskInfo.id) {
      return false;
    }

    return errors.equals(taskInfo.errors);
  }


  @Override
  public int hashCode() {
    int result = (int) (id ^ (id >>> 32));
    for (int i = 0; i < errors.size(); i++) {
      result = 31 * result + errors.get(i).hashCode();
    }
    return result;
  }
}
