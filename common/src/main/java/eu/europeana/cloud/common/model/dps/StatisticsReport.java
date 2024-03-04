package eu.europeana.cloud.common.model.dps;

import jakarta.xml.bind.annotation.XmlRootElement;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Model class for statistics report.
 */
@XmlRootElement
public class StatisticsReport {

  private long taskId;
  private List<NodeStatistics> nodeStatistics = new ArrayList<>();

  public StatisticsReport() {
  }

  public StatisticsReport(long taskId, List<NodeStatistics> nodeStatistics) {
    this.taskId = taskId;
    this.nodeStatistics = nodeStatistics;
  }

  public void setTaskId(long taskId) {
    this.taskId = taskId;
  }

  public long getTaskId() {
    return taskId;
  }

  public List<NodeStatistics> getNodeStatistics() {
    return nodeStatistics;
  }

  public void setNodeStatistics(List<NodeStatistics> nodeStatistics) {
    this.nodeStatistics = nodeStatistics;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StatisticsReport report = (StatisticsReport) o;
    return taskId == report.taskId &&
        Objects.equals(nodeStatistics, report.nodeStatistics);
  }

  @Override
  public int hashCode() {
    return Objects.hash(taskId, nodeStatistics);
  }
}
