package eu.europeana.cloud.enrichment.bolts;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.enrichment.rest.client.EnrichmentWorker;

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
        prepareStormTaskTupleForEmission(stormTaskTuple, output);
        outputCollector.emit(currentTuple,stormTaskTuple.toStormTuple());
    }

    @Override
    public void prepare() {
        enrichmentWorker = new EnrichmentWorker(dereferenceURL, enrichmentURL);
    }
}
