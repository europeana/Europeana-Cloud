package eu.europeana.cloud.enrichment.bolts;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.StormTaskTupleHelper;
import eu.europeana.enrichment.rest.client.EnrichmentWorker;
import eu.europeana.enrichment.rest.client.EnrichmentWorkerImpl;
import eu.europeana.enrichment.rest.client.dereference.DereferencerProvider;
import eu.europeana.enrichment.rest.client.enrichment.EnricherProvider;
import eu.europeana.enrichment.rest.client.exceptions.DereferenceException;
import eu.europeana.enrichment.rest.client.exceptions.EnrichmentException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Call the remote enrichment service in order to dereference and enrich a file.
 * <p/>
 * Receives a byte array representing a Record from a tuple, enrich its content nad store it as part
 * of the emitted tuple.
 */
public class EnrichmentBolt extends AbstractDpsBolt {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(EnrichmentBolt.class);

    private String dereferenceURL;
    private String enrichmentURL;
    private transient EnrichmentWorker enrichmentWorker;

    public EnrichmentBolt(String dereferenceURL, String enrichmentURL) {
        this.dereferenceURL = dereferenceURL;
        this.enrichmentURL = enrichmentURL;
    }

    @Override
    public void execute(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
        try {
            String fileContent = new String(stormTaskTuple.getFileData());
            LOGGER.info("starting enrichment on {} .....", stormTaskTuple.getFileUrl());
            String output = enrichmentWorker.process(fileContent);
            LOGGER.info("Finishing enrichment on {} .....", stormTaskTuple.getFileUrl());
            emitEnrichedContent(anchorTuple, stormTaskTuple, output);
        } catch (Exception e) {
            LOGGER.error("Exception while Enriching/dereference", e);
            emitErrorNotification(anchorTuple, stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(),
                    e.getMessage(),
                    "Remote Enrichment/dereference service caused the problem!. The full error: "
                            + ExceptionUtils.getStackTrace(e),
                    StormTaskTupleHelper.getRecordProcessingStartTime(stormTaskTuple));
        }
        outputCollector.ack(anchorTuple);
    }

    private void emitEnrichedContent(Tuple anchorTuple, StormTaskTuple stormTaskTuple, String output)
            throws Exception {
        prepareStormTaskTupleForEmission(stormTaskTuple, output);
        outputCollector.emit(anchorTuple, stormTaskTuple.toStormTuple());
    }

    @Override
    public void prepare() {
        final EnricherProvider enricherProvider = new EnricherProvider();
        enricherProvider.setEnrichmentUrl(enrichmentURL);
        final DereferencerProvider dereferencerProvider = new DereferencerProvider();
        dereferencerProvider.setDereferenceUrl(dereferenceURL);
        dereferencerProvider.setEnrichmentUrl(enrichmentURL);
        try {
            enrichmentWorker = new EnrichmentWorkerImpl(dereferencerProvider.create(),
                    enricherProvider.create());
        } catch (DereferenceException | EnrichmentException e) {
            throw new RuntimeException("Could not instantiate EnrichmentBolt due Exception in enrich worker creating",e);
        }
    }
}
