package eu.europeana.cloud.service.dps.storm.topologies.media.service;

import com.google.gson.Gson;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.util.LRUCache;
import eu.europeana.metis.mediaprocessing.RdfConverterFactory;
import eu.europeana.metis.mediaprocessing.RdfDeserializer;
import eu.europeana.metis.mediaprocessing.RdfSerializer;
import eu.europeana.metis.mediaprocessing.exception.RdfSerializationException;
import eu.europeana.metis.mediaprocessing.model.EnrichedRdf;
import eu.europeana.metis.mediaprocessing.model.ResourceMetadata;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;

/**
 * Created by Tarek on 12/12/2018.
 */
public class EDMEnrichmentBolt extends AbstractDpsBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(EDMEnrichmentBolt.class);
    private static final String MEDIA_RESOURCE_EXCEPTION = "media resource exception";

    private static final int CACHE_SIZE = 25;

    private Gson gson;
    private RdfDeserializer deserializer;
    private RdfSerializer rdfSerializer;

    static LRUCache<String, TempEnrichedFile> cache = new LRUCache<>(CACHE_SIZE);

    @Override
    public void execute(StormTaskTuple stormTaskTuple) {
        if (stormTaskTuple.getParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT) == null) {
            outputCollector.emit(stormTaskTuple.toStormTuple());
        } else {
            final String file = stormTaskTuple.getParameters().get(PluginParameterKeys.DPS_TASK_INPUT_DATA);
            TempEnrichedFile tempEnrichedFile = cache.get(file);
            try {
                if (tempEnrichedFile == null) {
                    tempEnrichedFile = new TempEnrichedFile();
                    tempEnrichedFile.setEnrichedRdf(deserializer.getRdfForResourceEnriching(stormTaskTuple.getFileData()));
                }

                if (stormTaskTuple.getParameter(PluginParameterKeys.RESOURCE_METADATA) != null) {
                    EnrichedRdf enrichedRdf = enrichRdf(tempEnrichedFile.getEnrichedRdf(), stormTaskTuple.getParameter(PluginParameterKeys.RESOURCE_METADATA));
                    String cachedErrorMessage = tempEnrichedFile.getExceptions();
                    cachedErrorMessage = buildErrorMessage(stormTaskTuple.getParameter(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE), cachedErrorMessage);
                    tempEnrichedFile.setEnrichedRdf(enrichedRdf);
                    tempEnrichedFile.setExceptions(cachedErrorMessage);
                }
            } catch (Exception e) {
                LOGGER.error("problem while enrichment ", e);
                String currentException = tempEnrichedFile.getExceptions();
                if (currentException.isEmpty())
                    tempEnrichedFile.setExceptions(e.getMessage());
                else
                    tempEnrichedFile.setExceptions(currentException + "," + e.getMessage());
            } finally {
                if (tempEnrichedFile.isTheLastResource(Integer.parseInt(stormTaskTuple.getParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT)))) {
                    try {
                        LOGGER.info("The file {} was fully enriched and will be send to the next bolt", file);
                        prepareStormTaskTuple(stormTaskTuple, tempEnrichedFile);
                        outputCollector.emit(stormTaskTuple.toStormTuple());
                    } catch (Exception ex) {
                        LOGGER.error("Error while serializing the enriched file: ", ex);
                        emitErrorNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), ex.getMessage(), "Error while serializing the enriched file: " + ExceptionUtils.getStackTrace(ex));
                    }
                } else {
                    tempEnrichedFile.increaseCount();
                    cache.put(file, tempEnrichedFile);
                }
            }
        }
    }

    private void prepareStormTaskTuple(StormTaskTuple stormTaskTuple, TempEnrichedFile tempEnrichedFile) throws RdfSerializationException, MalformedURLException {
        if (!tempEnrichedFile.getExceptions().isEmpty()) {
            stormTaskTuple.addParameter(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE, tempEnrichedFile.getExceptions());
            stormTaskTuple.addParameter(PluginParameterKeys.UNIFIED_ERROR_MESSAGE, MEDIA_RESOURCE_EXCEPTION);
        }
        stormTaskTuple.setFileData(rdfSerializer.serialize(tempEnrichedFile.getEnrichedRdf()));
        final UrlParser urlParser = new UrlParser(stormTaskTuple.getFileUrl());
        if (urlParser.isUrlToRepresentationVersionFile()) {
            stormTaskTuple
                    .addParameter(PluginParameterKeys.CLOUD_ID, urlParser.getPart(UrlPart.RECORDS));
            stormTaskTuple.addParameter(PluginParameterKeys.REPRESENTATION_NAME,
                    urlParser.getPart(UrlPart.REPRESENTATIONS));
            stormTaskTuple.addParameter(PluginParameterKeys.REPRESENTATION_VERSION,
                    urlParser.getPart(UrlPart.VERSIONS));
        }

        stormTaskTuple.getParameters().remove(PluginParameterKeys.RESOURCE_METADATA);
    }

    private String buildErrorMessage(String resourceErrorMessage, String cachedErrorMessage) {
        return cachedErrorMessage + handleResourceErrorMessage(resourceErrorMessage);
    }

    private String handleResourceErrorMessage(String resourceErrorMessage) {
        if (resourceErrorMessage == null)
            return "";
        return ", " + resourceErrorMessage;

    }

    private EnrichedRdf enrichRdf(EnrichedRdf enrichedRdf, String resourceMetaData) {
        ResourceMetadata resourceMetadata = gson.fromJson(resourceMetaData, ResourceMetadata.class);
        enrichedRdf.enrichResource(resourceMetadata);
        return enrichedRdf;
    }


    @Override
    public void prepare() {
        try {
            deserializer = new RdfConverterFactory().createRdfDeserializer();
            rdfSerializer = new RdfConverterFactory().createRdfSerializer();
            gson = new Gson();

        } catch (Exception e) {
            throw new RuntimeException("Error while creating serializer/deserializer", e);
        }
    }

    static class TempEnrichedFile {
        private EnrichedRdf enrichedRdf;
        private String exceptions;
        private int count;

        public TempEnrichedFile() {
            count = 0;
            exceptions = "";
        }

        public EnrichedRdf getEnrichedRdf() {
            return enrichedRdf;
        }

        public String getExceptions() {
            return exceptions;
        }

        public void setEnrichedRdf(EnrichedRdf enrichedRdf) {
            this.enrichedRdf = enrichedRdf;
        }

        public void setExceptions(String exceptions) {
            this.exceptions = exceptions;
        }

        public void increaseCount() {
            count++;
        }

        public int getCount() {
            return count;
        }

        public boolean isTheLastResource(int linkCount) {
            if (count + 1 == linkCount)
                return true;
            return false;
        }
    }
}
