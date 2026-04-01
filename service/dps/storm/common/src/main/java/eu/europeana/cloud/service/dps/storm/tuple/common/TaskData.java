package eu.europeana.cloud.service.dps.storm.tuple.common;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import java.io.Serializable;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
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
    Revision inputRevision;
    // Datasets could not be stored as DataSet object because of problems with serialization of java.net.URI field by Kryo library
    // used by Storm on current version of java and storm library
    String outputDatasetProvider;
    String outputDatasetId;
    String inputDatasetProvider;
    String inputDatasetId;

    public TaskData(long taskId, String taskName) {
        this.taskId = taskId;
        this.taskName = taskName;
    }

    public TaskData(long taskId, String taskName, String sentDate) {
        this(taskId, taskName);
        this.sentDate = sentDate;
    }

    public void setInputDatasetFromUri(String uri) throws MalformedURLException, URISyntaxException {
        DataSet dataset = parseDatasetUrl(uri);
        inputDatasetProvider = dataset.getProviderId();
        inputDatasetId = dataset.getId();
    }

    public void setOutputDatasetFromUri(String uri) throws MalformedURLException, URISyntaxException {
        setOutputDataset(parseDatasetUrl(uri));
    }

    public void setOutputDataset(DataSet outputDataset) {
        outputDatasetProvider = outputDataset.getProviderId();
        outputDatasetId = outputDataset.getId();
    }

    private DataSet parseDatasetUrl(String uri) throws MalformedURLException, URISyntaxException {
        UrlParser parser = new UrlParser(uri);
        if (parser.isUrlToDataset()) {
            DataSet dataSet = new DataSet();
            dataSet.setId(parser.getPart(UrlPart.DATA_SETS));
            dataSet.setProviderId(parser.getPart(UrlPart.DATA_PROVIDERS));
            dataSet.setUri(new URI(uri));
            return dataSet;
        } else {
            throw new MalformedURLException("Provided Uri doesn't point to dataset!");
        }
    }
}
