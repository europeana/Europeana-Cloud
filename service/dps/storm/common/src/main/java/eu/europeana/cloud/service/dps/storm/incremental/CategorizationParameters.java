package eu.europeana.cloud.service.dps.storm.incremental;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.time.Instant;
import java.util.UUID;

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
  private final UUID recordMd5;
  private final Instant recordDateStamp;
  private final Instant currentHarvestDate;
}
