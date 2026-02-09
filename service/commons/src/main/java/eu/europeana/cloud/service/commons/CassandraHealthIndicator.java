package eu.europeana.cloud.service.commons;

import com.datastax.driver.core.exceptions.DriverException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

/**
 * Verifies Cassandra connection for Actuator health endpoint
 */
public class CassandraHealthIndicator implements HealthIndicator {

  private static final Log LOGGER = LogFactory.getLog(CassandraHealthIndicator.class);

  private final CassandraConnectionProvider cassandraConnectionProvider;

  /**
   * Constructor for component.
   *
   * @param cassandraConnectionProvider cassandraConnectionProvider
   */
  public CassandraHealthIndicator(CassandraConnectionProvider cassandraConnectionProvider) {
    this.cassandraConnectionProvider = cassandraConnectionProvider;
  }

  @Override
  public Health health() {

    try {
      LOGGER.debug("Verifying Cassandra connection");
      cassandraConnectionProvider.getSession().execute("SELECT now() FROM system.local");
      return Health.up()
              .withDetail("clusterName", cassandraConnectionProvider.getSession().getCluster().getClusterName())
              .withDetail("nodes", cassandraConnectionProvider.getSession().getCluster().getMetadata().getAllHosts().size())
              .build();

    } catch (DriverException e) {
      LOGGER.error("Error while verifying Cassandra connection", e);
      return Health.down()
              .build();
    }
  }
}
