package eu.europeana.cloud.common.model.dps;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement()
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
    private String recordId;
    private String resultResource;

    public SubTaskInfo(String resource) {
        this(0, resource, null, null, null, null);
    }

    public SubTaskInfo(int resourceNum, String resource, RecordState recordState, String info, String additionalInformations, String recordId) {
        this(resourceNum, resource, recordState, info, additionalInformations, recordId, null);
    }
}
