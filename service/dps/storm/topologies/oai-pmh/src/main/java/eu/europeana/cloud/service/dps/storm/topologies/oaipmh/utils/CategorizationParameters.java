package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.utils;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.time.Instant;

/**
 * Encapsulates all needed parameters for records categorization in incremental harvesting
 */
@Getter
@Builder
@ToString
public class CategorizationParameters {

    private final boolean fullHarvest;
    private final String datasetId;
    private final String recordId;
    private final Instant recordDateStamp;
    private final Instant currentHarvestDate;
}
