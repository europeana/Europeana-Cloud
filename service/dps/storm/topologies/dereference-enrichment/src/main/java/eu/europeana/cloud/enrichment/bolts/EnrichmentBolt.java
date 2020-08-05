package eu.europeana.cloud.enrichment.bolts;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.enrichment.rest.client.DereferenceOrEnrichException;
import eu.europeana.enrichment.rest.client.EnrichmentWorker;
import eu.europeana.enrichment.rest.client.EnrichmentWorkerBuilder;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.Date;

/**
 * Call the remote enrichment service in order to dereference and enrich a file.
 * <p/>
 * Receives a byte array representing a Record from a tuple, enrich its content nad store it
 * as part of the emitted tuple.
 */
public class EnrichmentBolt extends AbstractDpsBolt {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(EnrichmentBolt.class);

    private final String dereferenceURL;
    private final String enrichmentURL;
    private transient EnrichmentWorker enrichmentWorker;

    public EnrichmentBolt(String dereferenceURL, String enrichmentURL) {
        this.dereferenceURL = dereferenceURL;
        this.enrichmentURL = enrichmentURL;
    }

    @Override
    public void execute(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
        try {
            long processingStartTime = new Date().getTime();
            String output = enrichFile(stormTaskTuple);
            emitEnrichedContent(anchorTuple, stormTaskTuple, output);
            LOGGER.info("Resource processing finished in: {}ms", (new Date().getTime() - processingStartTime));
        } catch (Exception e) {
            LOGGER.error("Exception while Enriching/dereference", e);
            emitErrorNotification(anchorTuple, stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), e.getMessage(), "Remote Enrichment/dereference service caused the problem!. The full error: " + ExceptionUtils.getStackTrace(e));
        }finally {
            outputCollector.ack(anchorTuple);
        }
    }

    private String enrichFile(StormTaskTuple stormTaskTuple) throws DereferenceOrEnrichException {
        String fileContent = new String(stormTaskTuple.getFileData());
        LOGGER.info("Starting enrichment on {} .....", stormTaskTuple.getFileUrl());
        String output = enrichmentWorker.process(fileContent);
        LOGGER.info("Finishing enrichment on {} .....", stormTaskTuple.getFileUrl());
        return output;
    }

    private void emitEnrichedContent(Tuple anchorTuple, StormTaskTuple stormTaskTuple, String output) throws MalformedURLException {
        prepareStormTaskTupleForEmission(stormTaskTuple, output);
        outputCollector.emit(anchorTuple, stormTaskTuple.toStormTuple());
    }

    @Override
    public void prepare() {
        enrichmentWorker = new EnrichmentWorkerBuilder()
                .setDereferenceUrl(dereferenceURL)
                .setEnrichmentUrl(enrichmentURL)
                .build();
    }
}
