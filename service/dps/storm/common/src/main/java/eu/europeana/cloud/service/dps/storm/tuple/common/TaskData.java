package eu.europeana.cloud.service.dps.storm.tuple.common;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Revision;
import java.io.Serializable;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Setter
@Getter
@NoArgsConstructor
public class TaskData implements Serializable {
    // Serializable needed for SerializationUtils.clone(commonTaskTuple)

    long taskId;
    String taskName;
    String sentDate;
    Map<String, String> parameters = new HashMap<>();

    Revision outputRevision;
    // Datasets could not be stored as DataSet object because of problems with serialization of java.net.URI field by Kryo library
    // used by Storm on current version of java and storm library
    String outputDatasetProvider;
    String outputDatasetId;
    String outputRepresentationName;

    public TaskData(long taskId, String taskName) {
        this.taskId = taskId;
        this.taskName = taskName;
    }

    public TaskData(long taskId, String taskName, String sentDate) {
        this(taskId, taskName);
        this.sentDate = sentDate;
    }

    public void setOutputDataset(DataSet outputDataset) {
        outputDatasetProvider = outputDataset.getProviderId();
        outputDatasetId = outputDataset.getId();
    }
}
