package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.Row;
import java.util.Date;
import java.util.UUID;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class HarvestedRecord {

  private String metisDatasetId;
  private String recordLocalId;
  private int bucketNumber;
  private Date latestHarvestDate;
  private UUID latestHarvestMd5;
  private Date previewHarvestDate;
  private UUID previewHarvestMd5;
  private Date publishedHarvestDate;
  private UUID publishedHarvestMd5;

  public static HarvestedRecord from(Row row) {
    return builder()
        .metisDatasetId(row.getString(CassandraTablesAndColumnsNames.HARVESTED_RECORD_METIS_DATASET_ID))
        .recordLocalId(row.getString(CassandraTablesAndColumnsNames.HARVESTED_RECORD_LOCAL_ID))
        .bucketNumber(row.getInt(CassandraTablesAndColumnsNames.HARVESTED_RECORD_BUCKET_NUMBER))
        .latestHarvestDate(row.getTimestamp(CassandraTablesAndColumnsNames.HARVESTED_RECORD_LATEST_HARVEST_DATE))
        .latestHarvestMd5(row.getUUID(CassandraTablesAndColumnsNames.HARVESTED_RECORD_LATEST_HARVEST_MD5))
        .previewHarvestDate(row.getTimestamp(CassandraTablesAndColumnsNames.HARVESTED_RECORD_PREVIEW_HARVEST_DATE))
        .previewHarvestMd5(row.getUUID(CassandraTablesAndColumnsNames.HARVESTED_RECORD_PREVIEW_HARVEST_MD5))
        .publishedHarvestDate(row.getTimestamp(CassandraTablesAndColumnsNames.HARVESTED_RECORD_PUBLISHED_HARVEST_DATE))
        .publishedHarvestMd5(row.getUUID(CassandraTablesAndColumnsNames.HARVESTED_RECORD_PUBLISHED_HARVEST_MD5))
        .build();
  }
}
