package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.throttling.ThrottlingAttributeGenerator;
import eu.europeana.cloud.service.dps.storm.utils.MediaThrottlingFractionEvaluator;
import eu.europeana.metis.mediaprocessing.exception.RdfDeserializationException;
import eu.europeana.metis.mediaprocessing.model.RdfResourceEntry;

import java.util.List;
import java.util.Optional;

public class ParseFileForMediaBolt extends ParseFileBolt {

    private ThrottlingAttributeGenerator generator;

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
        applyThrottling(tuple);
        return tuple;
    }

    @Override
    protected int getLinksCount(StormTaskTuple tuple, int resourcesCount) {
        return Integer.parseInt(tuple.getParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT));
    }


    private void applyThrottling(StormTaskTuple tuple) {
        tuple.setThrottlingAttribute(generator.generate(tuple.getTaskId(),
                MediaThrottlingFractionEvaluator.evalForResourceProcessing(readParallelizationParam(tuple))));
    }

    private Integer readParallelizationParam(StormTaskTuple tuple) {
        return Optional.ofNullable(tuple.getParameter(PluginParameterKeys.MAXIMUM_PARALLELIZATION))
                .map(Integer::parseInt).orElse(Integer.MAX_VALUE);
    }

    @Override
    public void prepare() {
        super.prepare();
        generator=new ThrottlingAttributeGenerator();
    }
}
