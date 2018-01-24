package eu.europeana.cloud.common.model.dps;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Objects;

/**
 * Model class for statistics report.
 */
@XmlRootElement()
public class StatisticsReport {

    private long taskId;

    public StatisticsReport() {
    }

    public StatisticsReport(long taskId) {
        this.taskId = taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public long getTaskId() {
        return taskId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatisticsReport that = (StatisticsReport) o;
        return taskId == that.taskId;
    }

    @Override
    public int hashCode() {

        return Objects.hash(taskId);
    }
}
