package eu.europeana.cloud.service.dps.storm.topologies.media.service;

import com.google.gson.Gson;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.metis.mediaprocessing.RdfConverterFactory;
import eu.europeana.metis.mediaprocessing.RdfDeserializer;
import eu.europeana.metis.mediaprocessing.RdfSerializer;
import eu.europeana.metis.mediaprocessing.exception.RdfSerializationException;
import eu.europeana.metis.mediaprocessing.model.EnrichedRdf;
import eu.europeana.metis.mediaprocessing.model.ResourceMetadata;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Tarek on 12/12/2018.
 */
public class EDMEnrichmentBolt extends ReadFileBolt {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(EDMEnrichmentBolt.class);
    private static final String MEDIA_RESOURCE_EXCEPTION = "media resource exception";

    private static final int CACHE_SIZE = 1024;

    private transient Gson gson;
    private transient RdfDeserializer deserializer;
    private transient RdfSerializer rdfSerializer;

    transient Map<String, TempEnrichedFile> cache = new HashMap<>(CACHE_SIZE);

    public EDMEnrichmentBolt(String mcsURL) {
        super(mcsURL);
    }

    @Override
    public void execute(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
        if (stormTaskTuple.getParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT) == null) {
            outputCollector.emit(stormTaskTuple.toStormTuple());
        } else {
            final String file = stormTaskTuple.getFileUrl();
            TempEnrichedFile tempEnrichedFile = cache.get(file);
            try {
                if (tempEnrichedFile == null) {
                    try (InputStream stream = getFileStreamByStormTuple(stormTaskTuple)) {
                        tempEnrichedFile = new TempEnrichedFile();
                        tempEnrichedFile.setEnrichedRdf(deserializer.getRdfForResourceEnriching(IOUtils.toByteArray(stream)));
                    }
                }

                if (stormTaskTuple.getParameter(PluginParameterKeys.RESOURCE_METADATA) != null) {
                    EnrichedRdf enrichedRdf = enrichRdf(tempEnrichedFile.getEnrichedRdf(), stormTaskTuple.getParameter(PluginParameterKeys.RESOURCE_METADATA));
                    tempEnrichedFile.setEnrichedRdf(enrichedRdf);
                }
                String cachedErrorMessage = tempEnrichedFile.getExceptions();
                cachedErrorMessage = buildErrorMessage(stormTaskTuple.getParameter(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE), cachedErrorMessage);
                tempEnrichedFile.setExceptions(cachedErrorMessage);

            } catch (Exception e) {
                LOGGER.error("problem while enrichment ", e);
                String currentException = tempEnrichedFile.getExceptions();
                String exceptionMessage = "Exception while enriching the original edm file with resource: " + stormTaskTuple.getParameter(PluginParameterKeys.RESOURCE_URL) + " because of: " + ExceptionUtils.getStackTrace(e);
                if (currentException.isEmpty())
                    tempEnrichedFile.setExceptions(exceptionMessage);
                else
                    tempEnrichedFile.setExceptions(currentException + "," + exceptionMessage);
            } finally {
                if (tempEnrichedFile.isTheLastResource(Integer.parseInt(stormTaskTuple.getParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT)))) {
                    try {
                        LOGGER.info("The file {} was fully enriched and will be send to the next bolt", file);
                        prepareStormTaskTuple(stormTaskTuple, tempEnrichedFile);
                        cache.remove(file);
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
        if (cachedErrorMessage.isEmpty())
            return handleResourceErrorMessage(resourceErrorMessage);
        else if (resourceErrorMessage != null)
            return cachedErrorMessage + ", " + resourceErrorMessage;
        return cachedErrorMessage;
    }

    private String handleResourceErrorMessage(String resourceErrorMessage) {
        if (resourceErrorMessage == null)
            return "";
        return resourceErrorMessage;

    }

    private EnrichedRdf enrichRdf(EnrichedRdf enrichedRdf, String resourceMetaData) {
        ResourceMetadata resourceMetadata = gson.fromJson(resourceMetaData, ResourceMetadata.class);
        enrichedRdf.enrichResource(resourceMetadata);
        return enrichedRdf;
    }


    @Override
    public void prepare() {
        super.prepare();
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
