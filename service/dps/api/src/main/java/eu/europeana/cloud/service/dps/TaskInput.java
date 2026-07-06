package eu.europeana.cloud.service.dps;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;

/**
 * Marker interface for task input definition
 */
@JsonTypeInfo(use = Id.NAME)
@JsonSubTypes({
    @JsonSubTypes.Type(value = OAIPMHHarvestingDetails.class, name = "oai"),
    @JsonSubTypes.Type(value = HttpHarvestingDetails.class, name = "http"),
    @JsonSubTypes.Type(value = BatchInfo.class, name = "batch")
})
public interface TaskInput {

}
