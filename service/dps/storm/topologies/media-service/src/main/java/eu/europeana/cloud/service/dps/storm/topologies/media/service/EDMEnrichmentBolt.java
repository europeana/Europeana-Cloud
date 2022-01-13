package eu.europeana.cloud.service.dps.storm.topologies.media.service;

import com.google.gson.Gson;
import eu.europeana.cloud.common.utils.Clock;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.dps.storm.utils.StormTaskTupleHelper;
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
import java.time.Instant;
import java.util.*;

public class EDMEnrichmentBolt extends ReadFileBolt {
    public static final String NO_RESOURCES_DETAILED_MESSAGE = "No resources in rdf file for which media could be extracted, neither main thumbinal or remaining resources for media extraction.";
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(EDMEnrichmentBolt.class);
    private static final String MEDIA_RESOURCE_EXCEPTION = "media resource exception";
    private static final String SERIALIZATION_EXCEPTION_MESSAGE = "Error while serializing the enriched file: ";

    private static final int CACHE_SIZE = 1024;
    transient Map<String, TempEnrichedFile> cache;
    private transient Gson gson;
    private transient RdfDeserializer deserializer;
    private transient RdfSerializer rdfSerializer;

    public EDMEnrichmentBolt(String mcsURL) {
        super(mcsURL);
    }

    @Override
    public void execute(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
        LOGGER.debug("Starting EDM enrichment");
        Instant processingStartTime = Instant.now();
        if (stormTaskTuple.getParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT) == null) {
            LOGGER.warn(NO_RESOURCES_DETAILED_MESSAGE);
            try (InputStream stream = getFileStreamByStormTuple(stormTaskTuple)) {
                EnrichedRdf enrichedRdf = deserializer.getRdfForResourceEnriching(IOUtils.toByteArray(stream));
                prepareStormTaskTuple(stormTaskTuple, enrichedRdf, NO_RESOURCES_DETAILED_MESSAGE);
                outputCollector.emit(anchorTuple, stormTaskTuple.toStormTuple());
            } catch (Exception ex) {
                LOGGER.error(SERIALIZATION_EXCEPTION_MESSAGE, ex);
                emitErrorNotification(anchorTuple, stormTaskTuple.getTaskId(), stormTaskTuple.isMarkedAsDeleted(),
                        stormTaskTuple.getFileUrl(), ex.getMessage(),
                        SERIALIZATION_EXCEPTION_MESSAGE + ExceptionUtils.getStackTrace(ex),
                        StormTaskTupleHelper.getRecordProcessingStartTime(stormTaskTuple));
            }
            outputCollector.ack(anchorTuple);
        } else {
            final String file = stormTaskTuple.getFileUrl();
            TempEnrichedFile tempEnrichedFile = cache.get(file);
            try {
                if ((tempEnrichedFile == null) || (tempEnrichedFile.getTaskId() != stormTaskTuple.getTaskId())) {
                    try (InputStream stream = getFileStreamByStormTuple(stormTaskTuple)) {
                        tempEnrichedFile = new TempEnrichedFile();
                        tempEnrichedFile.setTaskId(stormTaskTuple.getTaskId());
                        byte[] bytes = IOUtils.toByteArray(stream);
                        tempEnrichedFile.setEnrichedRdf(deserializer.getRdfForResourceEnriching(bytes));
                        LOGGER.debug("Loaded, and deserialized file, that is being enriched, bytes={}", bytes.length);
                    }
                }
                tempEnrichedFile.addSourceTuple(anchorTuple);
                String metadata = stormTaskTuple.getParameter(PluginParameterKeys.RESOURCE_METADATA);
                if (metadata != null) {
                    EnrichedRdf enrichedRdf = enrichRdf(tempEnrichedFile.getEnrichedRdf(), metadata);
                    tempEnrichedFile.setEnrichedRdf(enrichedRdf);
                }
                String cachedErrorMessage = tempEnrichedFile.getExceptions();
                cachedErrorMessage = buildErrorMessage(stormTaskTuple.getParameter(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE), cachedErrorMessage);
                tempEnrichedFile.setExceptions(cachedErrorMessage);
                LOGGER.debug("Enriched file in cache. Link index: {}, Exceptions: {}, metadata: {} ",
                        tempEnrichedFile.getCount() + 1, tempEnrichedFile.getExceptions(), metadata);
            } catch (Exception e) {
                LOGGER.error("problem while enrichment ", e);
                String currentException = tempEnrichedFile.getExceptions();
                String exceptionMessage = "Exception while enriching the original edm file with resource: " + stormTaskTuple.getParameter(PluginParameterKeys.RESOURCE_URL) + " because of: " + ExceptionUtils.getStackTrace(e);
                if (currentException.isEmpty())
                    tempEnrichedFile.setExceptions(exceptionMessage);
                else
                    tempEnrichedFile.setExceptions(currentException + "," + exceptionMessage);
            } finally {
                tempEnrichedFile.increaseCount();
                if (tempEnrichedFile.isTheLastResource(Integer.parseInt(stormTaskTuple.getParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT)))) {
                    try {
                        LOGGER.debug("The file was fully enriched and will be send to the next bolt");
                        prepareStormTaskTuple(stormTaskTuple, tempEnrichedFile);
                        cache.remove(file);
                        outputCollector.emit(anchorTuple, stormTaskTuple.toStormTuple());
                    } catch (Exception ex) {
                        LOGGER.error(SERIALIZATION_EXCEPTION_MESSAGE, ex);
                        emitErrorNotification(anchorTuple, stormTaskTuple.getTaskId(),
                                stormTaskTuple.isMarkedAsDeleted(), stormTaskTuple.getFileUrl(), ex.getMessage(),
                                SERIALIZATION_EXCEPTION_MESSAGE + ExceptionUtils.getStackTrace(ex),
                                StormTaskTupleHelper.getRecordProcessingStartTime(stormTaskTuple));
                    }
                    ackAllSourceTuplesForFile(tempEnrichedFile);
                } else {
                    cache.put(file, tempEnrichedFile);
                }
            }
        }
        LOGGER.info("EDM enrichment finished in {}ms", Clock.millisecondsSince(processingStartTime));
    }

    @Override
    public void prepare() {
        super.prepare();
        try {
            deserializer = new RdfConverterFactory().createRdfDeserializer();
            rdfSerializer = new RdfConverterFactory().createRdfSerializer();
            gson = new Gson();
            cache = new HashMap<>(CACHE_SIZE);
        } catch (Exception e) {
            throw new RuntimeException("Error while creating serializer/deserializer", e);
        }
    }

    private void prepareStormTaskTuple(StormTaskTuple stormTaskTuple, TempEnrichedFile tempEnrichedFile) throws RdfSerializationException, MalformedURLException {
        String errorMessage = tempEnrichedFile.getExceptions();
        EnrichedRdf enrichedRdf = tempEnrichedFile.getEnrichedRdf();
        prepareStormTaskTuple(stormTaskTuple, enrichedRdf, errorMessage);
    }

    private void prepareStormTaskTuple(StormTaskTuple stormTaskTuple, EnrichedRdf enrichedRdf, String errorMessage) throws RdfSerializationException, MalformedURLException {
        if (!errorMessage.isEmpty()) {
            stormTaskTuple.addParameter(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE, errorMessage);
            stormTaskTuple.addParameter(PluginParameterKeys.UNIFIED_ERROR_MESSAGE, MEDIA_RESOURCE_EXCEPTION);
        }
        stormTaskTuple.setFileData(rdfSerializer.serialize(enrichedRdf));
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

    private void ackAllSourceTuplesForFile(TempEnrichedFile enrichedFile) {
        for (Tuple tuple : enrichedFile.sourceTupples) {
            outputCollector.ack(tuple);
        }
    }

    static class TempEnrichedFile {
        private Long taskId;
        private EnrichedRdf enrichedRdf;
        private String exceptions;
        private int count;
        private List<Tuple> sourceTupples = new ArrayList<>();

        public TempEnrichedFile() {
            count = 0;
            exceptions = "";
        }

        public EnrichedRdf getEnrichedRdf() {
            return enrichedRdf;
        }

        public void setEnrichedRdf(EnrichedRdf enrichedRdf) {
            this.enrichedRdf = enrichedRdf;
        }

        public String getExceptions() {
            return exceptions;
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
            return (count == linkCount);
        }

        public void addSourceTuple(Tuple anchorTuple) {
            sourceTupples.add(anchorTuple);
        }

        public Long getTaskId() {
            return taskId;
        }

        public void setTaskId(Long taskId) {
            this.taskId = taskId;
        }

    }
}
