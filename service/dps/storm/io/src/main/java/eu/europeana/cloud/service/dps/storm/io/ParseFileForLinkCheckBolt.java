package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.metis.mediaprocessing.exception.RdfDeserializationException;
import eu.europeana.metis.mediaprocessing.model.RdfResourceEntry;

import java.util.List;

public class ParseFileForLinkCheckBolt extends ParseFileBolt {
    public ParseFileForLinkCheckBolt(String ecloudMcsAddress) {
        super(ecloudMcsAddress);
    }

    protected List<RdfResourceEntry> getResourcesFromRDF(byte[] bytes) throws RdfDeserializationException {
        // TODO Here we use deprecated method which should be changed to rdfDeserializer.getResourceEntriesForLinkChecking(bytes)
        return rdfDeserializer.getRemainingResourcesForMediaExtraction(bytes);
    }
}
