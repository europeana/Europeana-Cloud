package eu.europeana.cloud.common.model.dps;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Model class for statistics report.
 */
@XmlRootElement()
public class StatisticsReport {

    private long id;

    public StatisticsReport() {
    }

    public StatisticsReport(long taskId) {
        this.id = taskId;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatisticsReport that = (StatisticsReport) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {

        return Objects.hash(id);
    }
}
