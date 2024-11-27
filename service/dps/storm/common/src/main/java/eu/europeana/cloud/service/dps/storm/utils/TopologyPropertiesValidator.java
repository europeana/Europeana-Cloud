package eu.europeana.cloud.service.dps.storm.utils;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.MESSAGE_TIMEOUT_IN_SECONDS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_NAME;

import java.util.Properties;

public final class TopologyPropertiesValidator {

  private static final String ERROR_MESSAGE = "Missing topology property: ";

  private TopologyPropertiesValidator() {
  }

  public static void validateFor(String topologyName, Properties properties) {
    switch (topologyName) {
      case TopologiesNames.XSLT_TOPOLOGY:
        validateForXsltTopology(properties);
        break;
      case TopologiesNames.OAI_TOPOLOGY:
        validateForOaiTopology(properties);
        break;
      case TopologiesNames.INDEXING_TOPOLOGY:
        validateIndexingTopology(properties);
        break;
      case TopologiesNames.DEPUBLICATION_TOPOLOGY:
        validateDepublicationTopology(properties);
        break;
      default:
        throw new TopologyPropertiesException("Validator not found for given topology name: " + topologyName);
    }
  }

  private static void validateForXsltTopology(Properties properties) {
    validateCommonProps(properties);
  }

  private static void validateForOaiTopology(Properties properties) {
    validateCommonProps(properties);
  }

  private static void validateIndexingTopology(Properties properties) {
    validateCommonProps(properties);
  }

  private static void validateDepublicationTopology(Properties properties) {
    validateCommonProps(properties);
  }

  private static void validateCommonProps(Properties properties) {
    if (properties.getProperty(TOPOLOGY_NAME) == null) {
      throw new TopologyPropertiesException(ERROR_MESSAGE + TOPOLOGY_NAME);
    }
    if (properties.getProperty(MESSAGE_TIMEOUT_IN_SECONDS) == null) {
      throw new TopologyPropertiesException(ERROR_MESSAGE + MESSAGE_TIMEOUT_IN_SECONDS);
    }
  }
}
