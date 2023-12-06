package eu.europeana.cloud.common.model;

import java.util.Objects;

/**
 * Globally unique data set id. Data set id is unique only for a certain provider id, so the combination of those two identifiers
 * is globally unique.
 */
public class CompoundDataSetId {

  /**
   * Data set id (unique for provider).
   */
  private final String dataSetId;

  /**
   * Data set's owner (provider) id.
   */
  private final String dataSetProviderId;


  /**
   * Constructs CompoundDataSetId using given provider id and data set id.
   *
   * @param dataSetProviderId provider id
   * @param dataSetId data set id
   */
  public CompoundDataSetId(String dataSetProviderId, String dataSetId) {
    this.dataSetId = dataSetId;
    this.dataSetProviderId = dataSetProviderId;
  }


  public String getDataSetId() {
    return dataSetId;
  }


  public String getDataSetProviderId() {
    return dataSetProviderId;
  }


  @Override
  public int hashCode() {
    int hash = 7;
    hash = 89 * hash + Objects.hashCode(this.dataSetId);
    hash = 89 * hash + Objects.hashCode(this.dataSetProviderId);
    return hash;
  }


  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final CompoundDataSetId other = (CompoundDataSetId) obj;
    if (!Objects.equals(this.dataSetId, other.dataSetId)) {
      return false;
    }
    return Objects.equals(this.dataSetProviderId, other.dataSetProviderId);
  }


  @Override
  public String toString() {
    return "CompoundDataSetId{" + "dataSetId=" + dataSetId + ", dataSetProviderId=" + dataSetProviderId + '}';
  }

}
