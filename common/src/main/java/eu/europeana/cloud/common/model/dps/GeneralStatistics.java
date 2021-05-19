package eu.europeana.cloud.common.model.dps;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
public class GeneralStatistics {

    private String parentXpath;
    private String nodeXpath;
    private Long occurrence;

}
