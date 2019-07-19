package eu.europeana.cloud.enrichment.bolts;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.enrichment.rest.client.EnrichmentWorker;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Call the remote enrichment service in order to dereference and enrich a file.
 * <p/>
 * Receives a byte array representing a Record from a tuple, enrich its content nad store it
 * as part of the emitted tuple.
 */
public class EnrichmentBolt extends AbstractDpsBolt {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(EnrichmentBolt.class);

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
            LOGGER.info("starting enrichment on {} .....", stormTaskTuple.getFileUrl());
            String output = enrichmentWorker.process(fileContent);
            LOGGER.info("Finishing enrichment on {} .....", stormTaskTuple.getFileUrl());
            emitEnrichedContent(stormTaskTuple, output);
        } catch (Exception e) {
            LOGGER.error("Exception while Enriching/dereference", e);
            emitErrorNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), e.getMessage(), "Remote Enrichment/dereference service caused the problem!. The full error: " + ExceptionUtils.getStackTrace(e));
        }

    }

    private void emitEnrichedContent(StormTaskTuple stormTaskTuple, String output) throws Exception {
        prepareStormTaskTupleForEmission(stormTaskTuple, output);
        outputCollector.emit(stormTaskTuple.toStormTuple());
    }

    @Override
    public void prepare() {
        enrichmentWorker = new EnrichmentWorker(dereferenceURL, enrichmentURL);
    }
}
