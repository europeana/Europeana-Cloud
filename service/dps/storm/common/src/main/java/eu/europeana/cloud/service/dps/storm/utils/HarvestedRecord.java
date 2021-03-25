package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.Row;
import lombok.Builder;
import lombok.Data;

import java.util.Date;
import java.util.UUID;

@Data
@Builder
public class HarvestedRecord {

    private String metisDatasetId;
    private String recordLocalId;
    private Date harvestDate;
    private UUID md5;
    private Date indexingDate;

    public static HarvestedRecord from(Row row) {
        return builder()
                .metisDatasetId(row.getString(CassandraTablesAndColumnsNames.HARVESTED_RECORD_METIS_DATASET_ID))
                .recordLocalId(row.getString(CassandraTablesAndColumnsNames.HARVESTED_RECORD_LOCAL_ID))
                .harvestDate(row.getTimestamp(CassandraTablesAndColumnsNames.HARVESTED_RECORD_HARVEST_DATE))
                .md5(row.getUUID(CassandraTablesAndColumnsNames.HARVESTED_RECORD_MD5))
                .indexingDate(row.getTimestamp(CassandraTablesAndColumnsNames.HARVESTED_RECORD_INDEXING_DATE))
                .build();
    }
}
