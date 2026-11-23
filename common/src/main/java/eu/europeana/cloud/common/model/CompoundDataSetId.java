package eu.europeana.cloud.common.model;

import java.util.Set;
import lombok.Value;

/**
 * Globally unique data set id. Data set id is unique only for a certain provider id, so the combination of those two identifiers
 * is globally unique.
 */
@Value
public class CompoundDataSetId {


  /**
   * Data set's owner (provider) id.
   */
  String dataSetProviderId;
  /**
   * Data set id (unique for provider).
   */
  Set<String> dataSetIds;
}
