package eu.europeana.cloud.service.dps.storm.tuple.storm;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.enrichment.rest.client.report.Report;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Set;

@Setter
@Getter
@NoArgsConstructor
class ProcessingMetadata {

    Revision outputRevision;
    Revision inputRevision;
    DataSet outputDataset;
    DataSet inputDataset;


    Set<Report> reportSet;

    public ProcessingMetadata(Set<Report> reportSet) {
        this.reportSet = reportSet;
    }

    public void addReports(Collection<Report> reports) {
        this.reportSet.addAll(reports);
    }

    public void setInputDatasetFromUri(String uri) throws MalformedURLException, URISyntaxException {
        this.inputDataset = parseDatasetUrl(uri);
    }

    public void setOutputDatasetFromUri(String uri) throws MalformedURLException, URISyntaxException {
        this.outputDataset = parseDatasetUrl(uri);
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
