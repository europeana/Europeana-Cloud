package eu.europeana.cloud.service.mcs.persistent.cassandra;

import com.datastax.driver.core.Row;
import lombok.Builder;
import lombok.Data;

import java.util.UUID;

@Data
@Builder
public class DatasetAssignment {

    private String cloudId;
    private String version;
    private String schema;


    public static DatasetAssignment from(Row row) {
        return DatasetAssignment.builder()
                .cloudId(row.getString("cloud_id"))
                .version(row.getUUID("version_id").toString())
                .schema(row.getString("schema_id"))
                .build();
    }

}
