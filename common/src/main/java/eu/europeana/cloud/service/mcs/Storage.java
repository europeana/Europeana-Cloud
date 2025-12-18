package eu.europeana.cloud.service.mcs;

/**
 * Indicate storage.
 *
 * Enum values are used as key in Cassandra, so coudl not be changed easily
 *
 */
public enum Storage {
  OBJECT_STORAGE, // S3 Storage
  DATA_BASE, // old files_content Cassandra table - used by default
  DB_STORAGE // new files_content_v2 Cassandra table with optimization
}
