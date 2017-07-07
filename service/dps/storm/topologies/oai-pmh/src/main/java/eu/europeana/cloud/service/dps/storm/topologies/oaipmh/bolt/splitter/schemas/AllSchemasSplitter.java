package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.splitter.schemas;

import com.lyncode.xoai.model.oaipmh.MetadataFormat;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.splitter.Splitter;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by Tarek on 7/5/2017.
 */
public class AllSchemasSplitter extends SchemasSplitter {
    public AllSchemasSplitter(Splitter splitter) {
        super(splitter);
    }


    /**
     * List all the resource schemas and iterate over them to emit tuple per each schema.
     */
    public void split() {
        Iterator<MetadataFormat> metadataFormatIterator = splitter.getOaiHelper().listSchemas();
        OAIPMHHarvestingDetails oaipmhHarvestingDetails = splitter.getStormTaskTuple().getSourceDetails();
        Set<String> excludedSchemas = oaipmhHarvestingDetails.getExcludedSchemas();
        while (metadataFormatIterator.hasNext()) {
            String schema = metadataFormatIterator.next().getMetadataPrefix();
            if (excludedSchemas == null || !excludedSchemas.contains(schema)) {
                splitter.separateSchemaBySet(schema, oaipmhHarvestingDetails.getSets(), oaipmhHarvestingDetails.getDateFrom(), oaipmhHarvestingDetails.getDateUntil());
            }
        }

    }


}
