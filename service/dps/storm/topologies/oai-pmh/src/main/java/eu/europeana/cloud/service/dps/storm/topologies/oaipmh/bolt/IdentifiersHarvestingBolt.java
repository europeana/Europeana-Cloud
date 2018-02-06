package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt;


import com.rits.cloning.Cloner;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helpers.SourceProvider;
import org.dspace.xoai.model.oaipmh.Header;
import org.dspace.xoai.serviceprovider.exceptions.BadArgumentException;
import org.dspace.xoai.serviceprovider.exceptions.InvalidOAIResponse;
import org.dspace.xoai.serviceprovider.parameters.ListIdentifiersParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Set;

public class IdentifiersHarvestingBolt extends AbstractDpsBolt {
    private static final Logger LOGGER = LoggerFactory.getLogger(IdentifiersHarvestingBolt.class);

    private SourceProvider sourceProvider;


    /**
     * Harvest identifiers from the OAI-PMH source
     *
     * @param stormTaskTuple Tuple which DpsTask is part of ...
     */

    public void execute(StormTaskTuple stormTaskTuple) {
        try {
            if (stormTaskTuple.getSourceDetails() == null) {
                logAndEmitError(stormTaskTuple, "Harvesting details object is null!");
                return;
            }
            int count = harvestIdentifiers(stormTaskTuple);
            LOGGER.debug("Harvested " + count + " identifiers for source (" + stormTaskTuple.getSourceDetails() + ")");
        } catch (BadArgumentException | RuntimeException e) {
            LOGGER.error("Identifiers Harvesting Bolt error: {} \n StackTrace: \n{}", e.getMessage(), e.getStackTrace());
            logAndEmitError(stormTaskTuple, e.getMessage());
        }
    }

    private void emitIdentifier(StormTaskTuple stormTaskTuple, String identifier) {
        StormTaskTuple tuple = new Cloner().deepClone(stormTaskTuple);
        tuple.addParameter(PluginParameterKeys.OAI_IDENTIFIER, identifier);
        // set identifier as a file URL for notifications to work correctly
        tuple.setFileUrl(identifier);
        outputCollector.emit(inputTuple, tuple.toStormTuple());
    }

    @Override
    public void prepare() {
        sourceProvider = new SourceProvider();
    }

    /**
     * Validate parameters, init source and continue with ListIdentifiers request.
     *
     * @param stormTaskTuple tuple with harvesting details, it will also be used for emitting identifiers further
     * @throws BadArgumentException
     */
    private int harvestIdentifiers(StormTaskTuple stormTaskTuple)
            throws BadArgumentException {
        OAIPMHHarvestingDetails sourceDetails = stormTaskTuple.getSourceDetails();
        String url = stormTaskTuple.getParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA);
        validateParameters(url, sourceDetails);
        ListIdentifiersParameters parameters = configureParameters(sourceDetails);
        return parseHeaders(sourceProvider.provide(url).listIdentifiers(parameters), sourceDetails.getExcludedSets(), stormTaskTuple);
    }


    /**
     * Configure request parameters
     *
     * @param sourceDetails harvesting parameters
     * @return object representing parameters for ListIdentifiers request
     */
    private ListIdentifiersParameters configureParameters(OAIPMHHarvestingDetails sourceDetails) {
        ListIdentifiersParameters parameters = ListIdentifiersParameters.request().withMetadataPrefix(sourceDetails.getSchema());
        if (sourceDetails.getDateFrom() != null) {
            parameters.withFrom(sourceDetails.getDateFrom());
        }
        if (sourceDetails.getDateUntil() != null) {
            parameters.withUntil(sourceDetails.getDateUntil());
        }
        if (sourceDetails.getSet() != null) {
            parameters.withSetSpec(sourceDetails.getSet());
        }
        return parameters;
    }

    /**
     * Parse headers returned by the OAI-PMH source
     *
     * @param headerIterator iterator of headers returned by the source
     * @param excludedSets   sets to exclude
     * @param stormTaskTuple tuple to be used for emitting identifier
     * @return number of harvested identifiers
     */
    private int parseHeaders(Iterator<Header> headerIterator, Set<String> excludedSets, StormTaskTuple stormTaskTuple) {
        if (headerIterator == null) {
            throw new IllegalArgumentException("Header iterator is null");
        }

        int count = 0;
        while (hasNext(headerIterator)) {
            Header header = headerIterator.next();
            if (filterHeader(header, excludedSets)) {
                emitIdentifier(stormTaskTuple, header.getIdentifier());
                count++;
            }
        }
        return count;
    }

    private boolean hasNext(Iterator<Header> headerIterator) {
        int retries = DEFAULT_RETRIES;

        while (true) {
            try {
                return headerIterator.hasNext();
            }
            catch (InvalidOAIResponse e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error when harvesting identifiers. Retries left: " + retries);
                    try {
                        Thread.sleep(SLEEP_TIME);
                    } catch (InterruptedException e1) {
                        LOGGER.error(e1.getMessage());
                    }
                }
                else {
                    LOGGER.error("Harvesting identifiers failed.");
                    throw e;
                }
            }
        }
    }

    /**
     * Filter header by checking whether it belongs to any of excluded sets.
     *
     * @param header       header to filter
     * @param excludedSets sets to exclude
     */
    private boolean filterHeader(Header header, Set<String> excludedSets) {
        if (excludedSets != null && !excludedSets.isEmpty()) {
            for (String set : excludedSets) {
                if (header.getSetSpecs().contains(set)) {
                    return false;
                }
            }
        }
        return true;
    }


    /**
     * Validate parameters coming to this bolt.
     */
    private void validateParameters(String url, OAIPMHHarvestingDetails sourceDetails) {
        if (url == null) {
            throw new IllegalArgumentException("Source is not configured.");
        }

        if (sourceDetails.getSchema() == null) {
            throw new IllegalArgumentException("Schema is not specified.");
        }

        if (sourceDetails.getDateFrom() != null && sourceDetails.getDateUntil() != null && sourceDetails.getDateUntil().before(sourceDetails.getDateFrom())) {
            throw new IllegalArgumentException("Date until is earlier than the date from.");
        }
    }
}