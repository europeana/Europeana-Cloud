package eu.europeana.cloud.service.dps;

import lombok.*;

import java.io.Serializable;

@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class DpsRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private long taskId;
    private String recordId;
    private String metadataPrefix;
}