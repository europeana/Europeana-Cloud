package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.metis.mediaprocessing.exception.RdfDeserializationException;
import eu.europeana.metis.mediaprocessing.model.RdfResourceEntry;

import java.util.List;

public class ParseFileForMediaBolt extends ParseFileBolt {
    public ParseFileForMediaBolt(String ecloudMcsAddress) {
        super(ecloudMcsAddress);
    }

    protected List<RdfResourceEntry> getResourcesFromRDF(byte[] bytes) throws RdfDeserializationException {
        return rdfDeserializer.getRemainingResourcesForMediaExtraction(bytes);
    }

    @Override
    protected StormTaskTuple createStormTuple(StormTaskTuple stormTaskTuple, RdfResourceEntry rdfResourceEntry, int linksCount) {
        StormTaskTuple tuple = super.createStormTuple(stormTaskTuple, rdfResourceEntry, linksCount);
        tuple.addParameter(PluginParameterKeys.MAIN_THUMBNAIL_AVAILABLE, stormTaskTuple.getParameter(PluginParameterKeys.MAIN_THUMBNAIL_AVAILABLE));
        return tuple;
    }
}
