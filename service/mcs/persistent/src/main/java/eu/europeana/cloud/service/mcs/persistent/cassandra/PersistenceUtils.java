package eu.europeana.cloud.service.mcs.persistent.cassandra;

public final class PersistenceUtils {

  private PersistenceUtils() {
  }

  public static String createProviderDataSetId(String providerId, String dataSetId) {
    return providerId + CassandraDataSetDAO.CDSID_SEPARATOR + dataSetId;
  }

}
