package eu.europeana.cloud.service.dps.storm.utils;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.*;

import java.util.Properties;

public final class TopologyPropertiesValidator {

  private static final String ERROR_MESSAGE = "Missing topology property: ";
  private static final String NOT_INT_PROPERTY_MESSAGE = "Not integer property: ";

  private TopologyPropertiesValidator() {
  }

  public static void validateFor(String topologyName, Properties properties) {
    switch (topologyName) {
      case TopologiesNames.HTTP_TOPOLOGY:
        validateHarvestingTopology(properties);
        break;
      case TopologiesNames.XSLT_TOPOLOGY:
        validateForXsltTopology(properties);
        break;
      case TopologiesNames.OAI_TOPOLOGY:
        validateHarvestingTopology(properties);
        break;
      case TopologiesNames.VALIDATION_TOPOLOGY:
        validateValidationTopology(properties);
        break;
      case TopologiesNames.NORMALIZATION_TOPOLOGY:
        validateNormalizationTopology(properties);
        break;
      case TopologiesNames.ENRICHMENT_TOPOLOGY:
        validateEnrichmentTopology(properties);
        break;
      case TopologiesNames.MEDIA_TOPOLOGY:
        validateMediaTopology(properties);
        break;
      case TopologiesNames.INDEXING_TOPOLOGY:
        validateIndexingTopology(properties);
        break;
      case TopologiesNames.DEPUBLICATION_TOPOLOGY:
        validateDepublicationTopology(properties);
        break;
      case TopologiesNames.LINKCHECK_TOPOLOGY:
        validateLinkCheckTopology(properties);
        break;
      default:
        throw new TopologyPropertiesException("Validator not found for given topology name: " + topologyName);
    }
  }

  private static void validateHarvestingTopology(Properties properties) {
    validateCommonProps(properties);

    validateRequired(properties, MCS_URL);
    validateRequired(properties, UIS_URL);

    validateRequiredInt(properties, RECORD_HARVESTING_BOLT_PARALLEL);
    validateRequiredInt(properties, CATEGORIZATION_BOLT_PARALLEL);
    validateRequiredInt(properties, WRITE_BOLT_PARALLEL);
    validateRequiredInt(properties, REVISION_WRITER_BOLT_PARALLEL);
    validateRequiredInt(properties, DUPLICATES_BOLT_PARALLEL);

    validateRequiredInt(properties, RECORD_HARVESTING_BOLT_NUMBER_OF_TASKS);
    validateRequiredInt(properties, CATEGORIZATION_BOLT_NUMBER_OF_TASKS);
    validateRequiredInt(properties, WRITE_BOLT_NUMBER_OF_TASKS);
    validateRequiredInt(properties, REVISION_WRITER_BOLT_NUMBER_OF_TASKS);
    validateRequiredInt(properties, DUPLICATES_BOLT_NUMBER_OF_TASKS);
  }

  private static void validateForXsltTopology(Properties properties) {
    validateCommonProps(properties);

    validateRequired(properties, MCS_URL);

    validateRequiredInt(properties, RETRIEVE_FILE_BOLT_PARALLEL);
    validateRequiredInt(properties, XSLT_BOLT_PARALLEL);
    validateRequiredInt(properties, WRITE_BOLT_PARALLEL);
    validateRequiredInt(properties, REVISION_WRITER_BOLT_PARALLEL);

    validateRequiredInt(properties, RETRIEVE_FILE_BOLT_NUMBER_OF_TASKS);
    validateRequiredInt(properties, XSLT_BOLT_NUMBER_OF_TASKS);
    validateRequiredInt(properties, WRITE_BOLT_NUMBER_OF_TASKS);
    validateRequiredInt(properties, REVISION_WRITER_BOLT_NUMBER_OF_TASKS);
  }

  private static void validateValidationTopology(Properties properties) {
    validateCommonProps(properties);

    validateRequired(properties, MCS_URL);

    validateRequiredInt(properties, RETRIEVE_FILE_BOLT_PARALLEL);
    validateRequiredInt(properties, VALIDATION_BOLT_PARALLEL);
    validateRequiredInt(properties, REVISION_WRITER_BOLT_PARALLEL);
    validateRequiredInt(properties, STATISTICS_BOLT_PARALLEL);
    validateRequiredInt(properties, RETRIEVE_FILE_BOLT_NUMBER_OF_TASKS);
    validateRequiredInt(properties, VALIDATION_BOLT_NUMBER_OF_TASKS);
    validateRequiredInt(properties, REVISION_WRITER_BOLT_NUMBER_OF_TASKS);
    validateRequiredInt(properties, STATISTICS_BOLT_NUMBER_OF_TASKS);
  }

  private static void validateNormalizationTopology(Properties properties) {
    validateCommonProps(properties);

    validateRequired(properties, MCS_URL);

    validateRequiredInt(properties, RETRIEVE_FILE_BOLT_PARALLEL);
    validateRequiredInt(properties, NORMALIZATION_BOLT_PARALLEL);
    validateRequiredInt(properties, WRITE_BOLT_PARALLEL);
    validateRequiredInt(properties, REVISION_WRITER_BOLT_PARALLEL);

    validateRequiredInt(properties, RETRIEVE_FILE_BOLT_NUMBER_OF_TASKS);
    validateRequiredInt(properties, NORMALIZATION_BOLT_NUMBER_OF_TASKS);
    validateRequiredInt(properties, WRITE_BOLT_NUMBER_OF_TASKS);
    validateRequiredInt(properties, REVISION_WRITER_BOLT_NUMBER_OF_TASKS);
  }

  private static void validateEnrichmentTopology(Properties properties) {
    validateCommonProps(properties);

    validateRequired(properties, MCS_URL);

    validateRequiredInt(properties, RETRIEVE_FILE_BOLT_PARALLEL);
    validateRequiredInt(properties, ENRICHMENT_BOLT_PARALLEL);
    validateRequiredInt(properties, WRITE_BOLT_PARALLEL);
    validateRequiredInt(properties, REVISION_WRITER_BOLT_PARALLEL);

    validateRequiredInt(properties, RETRIEVE_FILE_BOLT_NUMBER_OF_TASKS);
    validateRequiredInt(properties, ENRICHMENT_BOLT_NUMBER_OF_TASKS);
    validateRequiredInt(properties, WRITE_BOLT_NUMBER_OF_TASKS);
    validateRequiredInt(properties, REVISION_WRITER_BOLT_NUMBER_OF_TASKS);

    validateRequired(properties, DEREFERENCE_SERVICE_URL);
    validateRequired(properties, ENRICHMENT_ENTITY_MANAGEMENT_URL);
    validateRequired(properties, ENRICHMENT_ENTITY_API_URL);
    validateRequired(properties, ENRICHMENT_ENTITY_API_KEY);

  }

  private static void validateMediaTopology(Properties properties) {
    validateCommonProps(properties);

    validateRequired(properties, MCS_URL);

    validateRequiredInt(properties, EDM_OBJECT_PROCESSOR_BOLT_PARALLEL);
    validateRequiredInt(properties, PARSE_FILE_BOLT_PARALLEL);
    validateRequiredInt(properties, RESOURCE_PROCESSING_BOLT_PARALLEL);
    validateRequiredInt(properties, EDM_ENRICHMENT_BOLT_PARALLEL);
    validateRequiredInt(properties, WRITE_BOLT_PARALLEL);
    validateRequiredInt(properties, REVISION_WRITER_BOLT_PARALLEL);

    validateRequiredInt(properties, EDM_OBJECT_PROCESSOR_BOLT_NUMBER_OF_TASKS);
    validateRequiredInt(properties, PARSE_FILE_BOLT_BOLT_NUMBER_OF_TASKS);
    validateRequiredInt(properties, RESOURCE_PROCESSING_BOLT_NUMBER_OF_TASKS);
    validateRequiredInt(properties, EDM_ENRICHMENT_BOLT_NUMBER_OF_TASKS);
    validateRequiredInt(properties, WRITE_BOLT_NUMBER_OF_TASKS);
    validateRequiredInt(properties, REVISION_WRITER_BOLT_NUMBER_OF_TASKS);

    validateRequired(properties, AWS_CREDENTIALS_ACCESSKEY);
    validateRequired(properties, AWS_CREDENTIALS_SECRETKEY);
    validateRequired(properties, AWS_CREDENTIALS_ENDPOINT);
    validateRequired(properties, AWS_CREDENTIALS_BUCKET);
  }

  private static void validateIndexingTopology(Properties properties) {
    validateCommonProps(properties);

    validateRequired(properties, MCS_URL);
    validateRequired(properties, UIS_URL);

    validateRequiredInt(properties, WRITE_BOLT_PARALLEL);
    validateRequiredInt(properties, RETRIEVE_FILE_BOLT_PARALLEL);
    validateRequiredInt(properties, INDEXING_BOLT_PARALLEL);
    validateRequiredInt(properties, REVISION_WRITER_BOLT_PARALLEL);

    validateRequiredInt(properties, WRITE_BOLT_NUMBER_OF_TASKS);
    validateRequiredInt(properties, INDEXING_BOLT_NUMBER_OF_TASKS);
    validateRequiredInt(properties, RETRIEVE_FILE_BOLT_NUMBER_OF_TASKS);
    validateRequiredInt(properties, REVISION_WRITER_BOLT_NUMBER_OF_TASKS);
  }

  private static void validateDepublicationTopology(Properties properties) {
    validateCommonProps(properties);

    validateRequiredInt(properties, DEPUBLICATION_BOLT_PARALLEL);
    validateRequiredInt(properties, NOTIFICATION_BOLT_PARALLEL);
    validateRequiredInt(properties, DEPUBLICATION_BOLT_NUMBER_OF_TASKS);
  }

  private static void validateLinkCheckTopology(Properties properties) {
    validateCommonProps(properties);

    validateRequired(properties, MCS_URL);

    validateRequiredInt(properties, PARSE_FILE_BOLT_PARALLEL);
    validateRequiredInt(properties, LINK_CHECK_BOLT_PARALLEL);
    validateRequiredInt(properties, PARSE_FILE_BOLT_BOLT_NUMBER_OF_TASKS);
    validateRequiredInt(properties, LINK_CHECK_BOLT_NUMBER_OF_TASKS);
  }

  private static void validateCommonProps(Properties properties) {
    validateRequired(properties, TOPOLOGY_NAME);
    validateRequiredInt(properties, WORKER_COUNT);
    validateRequiredInt(properties, THRIFT_PORT);
    validateRequired(properties, NIMBUS_SEEDS);

    validateRequired(properties, BOOTSTRAP_SERVERS);
    validateRequired(properties, TOPICS);

    validateRequired(properties, TOPOLOGY_USER_NAME);
    validateRequired(properties, TOPOLOGY_USER_PASSWORD);

    validateRequired(properties, CASSANDRA_HOSTS);
    validateRequiredInt(properties, CASSANDRA_PORT);
    validateRequired(properties, CASSANDRA_KEYSPACE_NAME);
    validateRequired(properties, CASSANDRA_USERNAME);
    validateRequired(properties, CASSANDRA_SECRET_TOKEN);

    validateRequiredInt(properties, NOTIFICATION_BOLT_PARALLEL);
    validateRequiredInt(properties, NOTIFICATION_BOLT_NUMBER_OF_TASKS);

    validateRequiredInt(properties, MESSAGE_TIMEOUT_IN_SECONDS);
    validateInt(properties, MAX_SPOUT_PENDING);
    validateInt(properties, MAX_TASK_PARALLELISM);
  }

  private static void validateRequired(Properties properties, String propertyName) {
    if (properties.getProperty(propertyName) == null) {
      throw new TopologyPropertiesException(ERROR_MESSAGE + propertyName);
    }
  }

  private static void validateRequiredInt(Properties properties, String propertyName) {
    validateRequired(properties, propertyName);
    validateInt(properties, propertyName);
  }

  private static void validateInt(Properties properties, String propertyName) {
    try {
      String stringValue = properties.getProperty(propertyName);
      if (stringValue != null) {
        Integer.parseInt(stringValue);
      }
    } catch (NumberFormatException e) {
      throw new TopologyPropertiesException(NOT_INT_PROPERTY_MESSAGE + propertyName, e);
    }
  }

}
