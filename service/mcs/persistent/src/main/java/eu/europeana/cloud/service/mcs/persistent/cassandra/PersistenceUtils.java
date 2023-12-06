package eu.europeana.cloud.service.mcs.persistent.cassandra;

import eu.europeana.cloud.common.model.CompoundDataSetId;

public final class PersistenceUtils {

  private PersistenceUtils() {
  }

  public static String createProviderDataSetId(String providerId, String dataSetId) {
    return providerId + CassandraDataSetDAO.CDSID_SEPARATOR + dataSetId;
  }

  public static CompoundDataSetId createCompoundDataSetId(String providerDataSetId) {
    String[] values = providerDataSetId.split(CassandraDataSetDAO.CDSID_SEPARATOR);
    if (values.length != 2) {
      throw new IllegalArgumentException(
          "Cannot construct proper compound data set id from value: "
              + providerDataSetId);
    }
    return new CompoundDataSetId(values[0], values[1]);
  }
}
