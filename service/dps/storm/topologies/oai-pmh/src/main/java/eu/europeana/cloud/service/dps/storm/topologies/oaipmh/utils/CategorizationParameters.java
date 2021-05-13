package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.utils;

import lombok.Builder;
import lombok.Getter;

import java.util.Date;

/**
 * Encapsulates all needed parameters for records categorization in incremental harvesting
 */
@Getter
@Builder
public class CategorizationParameters {

    private final String datasetId;
    private final String recordId;
    private final Date recordDateStamp;
    private final Date currentHarvestDate;
}
