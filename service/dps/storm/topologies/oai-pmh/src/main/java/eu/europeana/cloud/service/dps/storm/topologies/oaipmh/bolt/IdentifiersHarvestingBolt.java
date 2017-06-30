package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt;

import com.lyncode.xoai.model.oaipmh.Header;
import com.lyncode.xoai.serviceprovider.ServiceProvider;
import com.lyncode.xoai.serviceprovider.client.HttpOAIClient;
import com.lyncode.xoai.serviceprovider.client.OAIClient;
import com.lyncode.xoai.serviceprovider.exceptions.BadArgumentException;
import com.lyncode.xoai.serviceprovider.model.Context;
import com.lyncode.xoai.serviceprovider.parameters.ListIdentifiersParameters;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.OAIPMHSourceDetails;
import org.apache.storm.task.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class IdentifiersHarvestingBolt extends AbstractDpsBolt {
    public static final Logger LOGGER = LoggerFactory.getLogger(IdentifiersHarvestingBolt.class);

    /**
     * Harvest identifiers from the OAI-PMH source
     *
     * @param stormTaskTuple Tuple which DpsTask is part of ...
     */

    public void execute(StormTaskTuple stormTaskTuple) {
        try {
            OAIPMHSourceDetails sourceDetails = stormTaskTuple.getSourceDetails();
            List<String> identifiers = harvestIdentifiers(sourceDetails);
            LOGGER.debug("Harvested " + identifiers.size() + " identifiers for source (" + sourceDetails + ")");
            for (String identifier : identifiers) {
                stormTaskTuple.addParameter(PluginParameterKeys.OAI_IDENTIFIER, identifier);
                outputCollector.emit(inputTuple, stormTaskTuple.toStormTuple());
            }
        } catch (BadArgumentException | RuntimeException e) {
            LOGGER.error("Identifiers Harvesting Bolt error: {} \n StackTrace: \n{}", e.getMessage(), e.getStackTrace());
            logAndEmitError(stormTaskTuple, e.getMessage());
        }
    }

    @Override
    public void prepare() {
    }

    /**
     * Validate parameters, init source and continue with ListIdentifiers request.
     *
     * @param sourceDetails details of the source to harvest
     * @return list of OAI identfiers
     * @throws BadArgumentException
     */
    private List<String> harvestIdentifiers(OAIPMHSourceDetails sourceDetails)
            throws BadArgumentException {
        validateParameters(sourceDetails.getUrl(), sourceDetails.getSchema(), sourceDetails.getSets(), sourceDetails.getExcludedSets(), sourceDetails.getDateFrom(), sourceDetails.getDateUntil());

        OAIClient client = new HttpOAIClient(sourceDetails.getUrl());
        ServiceProvider source = new ServiceProvider(new Context().withOAIClient(client));
        return listIdentifiers(source, sourceDetails.getSchema(), sourceDetails.getSets(), sourceDetails.getExcludedSets(), sourceDetails.getDateFrom(), sourceDetails.getDateUntil());
    }

    /**
     * Configure the request parameters and run the ListIdentifiers request
     *
     * @param source configured OAI-PMH source
     * @param schema schema to harvest, mandatory parameter
     * @param sets sets to harvest, optional parameter
     * @param excludedSets sets to exclude, optional parameter
     * @param dateFrom date from, optional parameter
     * @param dateUntil date until, optional parameter
     * @return list of OAI identifiers
     * @throws BadArgumentException
     */
    private List<String> listIdentifiers(ServiceProvider source, String schema, Set<String> sets, Set<String> excludedSets, Date dateFrom, Date dateUntil)
            throws BadArgumentException {
        List<String> identifiers = new ArrayList<>();

        ListIdentifiersParameters parameters = configureParameters(schema, dateFrom, dateUntil);
        if (sets != null && !sets.isEmpty()) {
            for (String set : sets) {
                parameters.withSetSpec(set);
                identifiers.addAll(parseHeaders(source.listIdentifiers(parameters), null));
            }
        } else {
            identifiers.addAll(parseHeaders(source.listIdentifiers(parameters), excludedSets));
        }
        return identifiers;
    }

    /**
     * Configure request parameters
     *
     * @param schema schema to harvest, mandatory parameter
     * @param dateFrom date from, optional parameter
     * @param dateUntil date until, optional parameter
     * @return object representing parameters for ListIdentifiers request
     */
    private ListIdentifiersParameters configureParameters(String schema, Date dateFrom, Date dateUntil) {
        ListIdentifiersParameters parameters = ListIdentifiersParameters.request().withMetadataPrefix(schema);
        if (dateFrom != null) {
            parameters.withFrom(dateFrom);
        }
        if (dateUntil != null) {
            parameters.withUntil(dateUntil);
        }
        return parameters;
    }

    /**
     * Parse headers returned by the OAI-PMH source
     *
     * @param headerIterator iterator of headers returned by the source
     * @param excludedSets sets to exclude
     * @return list of OAI identifiers
     */
    private List<String> parseHeaders(Iterator<Header> headerIterator, Set<String> excludedSets) {
        List<String> filteredIdentifiers = new ArrayList<>();

        if (headerIterator == null) {
            throw new IllegalArgumentException("Header iterator is null");
        }

        while (headerIterator.hasNext()) {
            Header header = headerIterator.next();
            filterHeader(header, excludedSets, filteredIdentifiers);
        }

        return filteredIdentifiers;
    }

    /**
     * Filter header by checking whether it belongs to any of excluded sets.
     *
     * @param header header to filter
     * @param excludedSets sets to exclude
     * @param filteredIdentifiers result list where the identifier will be added if not filtered
     */
    private void filterHeader(Header header, Set<String> excludedSets, List<String> filteredIdentifiers) {
        if (excludedSets != null && !excludedSets.isEmpty()) {
            for (String set : excludedSets) {
                if (header.getSetSpecs().contains(set)) {
                    return;
                }
            }
        }
        filteredIdentifiers.add(header.getIdentifier());
    }


    /**
     * Validate parameters coming to this bolt.
     *
     * @param sourceUrl OAI-PMH source URL, mandatory parameter
     * @param schema schema to harvest, mandatory parameter
     * @param sets sets to harvest, optional parameter
     * @param excludedSets sets to exclude, optional parameter
     * @param dateFrom date from, optional parameter
     * @param dateUntil date until, optional parameter
     */
    private void validateParameters(String sourceUrl, String schema, Set<String> sets, Set<String> excludedSets, Date dateFrom, Date dateUntil) {
        if (sourceUrl == null) {
            throw new IllegalArgumentException("Source is not configured.");
        }

        if (schema == null) {
            throw new IllegalArgumentException("Schema is not specified.");
        }

        if (sets != null && !sets.isEmpty() && excludedSets != null && !excludedSets.isEmpty()) {
            throw new IllegalArgumentException("Included set(s) and excluded set(s) cannot be specified at the same time.");
        }

        if (dateFrom != null && dateUntil != null && dateUntil.before(dateFrom)) {
            throw new IllegalArgumentException("Date until is earlier than the date from.");
        }
    }


    /**
     * Should be used only on tests.
     */
    public static IdentifiersHarvestingBolt getTestInstance(OutputCollector outputCollector) {
        IdentifiersHarvestingBolt instance = new IdentifiersHarvestingBolt();
        instance.outputCollector = outputCollector;
        return instance;
    }
}