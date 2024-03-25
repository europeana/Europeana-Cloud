package eu.europeana.cloud.common.model.dps;

import jakarta.xml.bind.annotation.XmlRootElement;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@XmlRootElement
@AllArgsConstructor
@Builder
@Data
@NoArgsConstructor
public class SubTaskInfo {

  private int resourceNum;
  private String resource;
  private RecordState recordState;
  private String info;
  private String additionalInformations;
  private String europeanaId;
  private long processingTime;
  private String resultResource;

  public SubTaskInfo(String resource) {
    this(0, resource, null, null, null, null, 0L);
  }

  public SubTaskInfo(int resourceNum, String resource, RecordState recordState, String info,
      String additionalInformation, String europeanaId, long processingTime) {
    this(resourceNum, resource, recordState, info, additionalInformation, europeanaId, processingTime, null);
  }
}

