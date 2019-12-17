package eu.europeana.cloud.service.dps;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Date;

/**
 * Describing one execution of harvest of OAI Endpoint
 */
@Builder
@Getter
@ToString
public class Harvest implements Serializable {

    private static final long serialVersionUID = 1L;

    private String url;
    private String metadataPrefix;
    private Date from;
    private Date until;
    private String setSpec;
}
