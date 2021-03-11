package eu.europeana.cloud.service.dps.storm.utils;

import lombok.Builder;
import lombok.Data;

import java.util.Date;
import java.util.UUID;

@Data
@Builder
public class HarvestedRecord {

    private String providerId;
    private String datasetId;
    private String oaiId;
    private Date harvestDate;
    private Date indexingDate;
    private UUID md5;
    private boolean ignored;

}
