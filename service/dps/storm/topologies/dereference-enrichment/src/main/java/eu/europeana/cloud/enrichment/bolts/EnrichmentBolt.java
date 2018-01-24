package eu.europeana.cloud.enrichment.bolts;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.corelib.definitions.jibx.RDF;
import eu.europeana.enrichment.rest.client.EnrichmentWorker;
import eu.europeana.enrichment.utils.EnrichmentUtils;
import eu.europeana.metis.dereference.DereferenceUtils;

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
            RDF input = DereferenceUtils.toRDF(fileContent);
            RDF output = enrichmentWorker.process(input);
            emitEnrichedContent(stormTaskTuple, output);
        } catch (Exception e) {
            emitErrorNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), e.getMessage(), "Remote Enrichment/dereference service caused the problem!");
        }

    }

    private void emitEnrichedContent(StormTaskTuple stormTaskTuple, RDF output) throws Exception {
        String resultString = EnrichmentUtils.convertRDFtoString(output);
        stormTaskTuple.setFileData(resultString.getBytes(Charset.forName("UTF-8")));
        outputCollector.emit(inputTuple, stormTaskTuple.toStormTuple());
    }

    @Override
    public void prepare() {
        enrichmentWorker = new EnrichmentWorker(dereferenceURL, enrichmentURL);
    }
}
