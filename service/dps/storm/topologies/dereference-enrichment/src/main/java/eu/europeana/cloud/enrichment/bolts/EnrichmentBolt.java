package eu.europeana.cloud.enrichment.bolts;

import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.enrichment.rest.client.EnrichmentWorker;


import java.net.MalformedURLException;
import java.nio.charset.Charset;


/**
 * Call the remote enrichment service in order to dereference and enrich a file.
 * <p/>
 * Receives a byte array representing a Record from a tuple, enrich its content nad store it
 * as part of the emitted tuple.
 */

public class EnrichmentBolt extends AbstractDpsBolt {
    private String dereferenceURL;
    private String enrichmentURL;
    private EnrichmentWorker enrichmentWorker;

    public EnrichmentBolt(String dereferenceURL, String enrichmentURL) {
        this.dereferenceURL = dereferenceURL;
        this.enrichmentURL = enrichmentURL;
    }

    @Override
    public void execute(StormTaskTuple stormTaskTuple) {
        try {
            String fileContent = new String(stormTaskTuple.getFileData());
            String output = enrichmentWorker.process(fileContent);
            emitEnrichedContent(stormTaskTuple, output);
        } catch (Exception e) {
            emitErrorNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), e.getMessage(), "Remote Enrichment/dereference service caused the problem!");
        }

    }

    private void emitEnrichedContent(StormTaskTuple stormTaskTuple, String output) throws Exception {
        prepareStormTaskTuple(stormTaskTuple, output);
        outputCollector.emit(inputTuple, stormTaskTuple.toStormTuple());
    }

    private void prepareStormTaskTuple(StormTaskTuple stormTaskTuple, String resultString) throws MalformedURLException {
        stormTaskTuple.setFileData(resultString.getBytes(Charset.forName("UTF-8")));
        final UrlParser urlParser = new UrlParser(stormTaskTuple.getFileUrl());
        stormTaskTuple.addParameter(PluginParameterKeys.CLOUD_ID, urlParser.getPart(UrlPart.RECORDS));
        stormTaskTuple.addParameter(PluginParameterKeys.REPRESENTATION_NAME, urlParser.getPart(UrlPart.REPRESENTATIONS));
        stormTaskTuple.addParameter(PluginParameterKeys.REPRESENTATION_VERSION, urlParser.getPart(UrlPart.VERSIONS));

    }

    @Override
    public void prepare() {
        enrichmentWorker = new EnrichmentWorker(dereferenceURL, enrichmentURL);
    }
}
