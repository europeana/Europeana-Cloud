package eu.europeana.cloud.common.model.dps;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Date;

@XmlRootElement()
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TaskInfo {

    private long id;
    private String topologyName;
    private TaskState state;
    private String stateDescription;
    private Date sentTimestamp;
    private Date startTimestamp;
    private Date finishTimestamp;
    private int expectedRecordsNumber;
    private int processedRecordsCount;
    /** Number of records that was already processed by the topology that was ignored by this execution.<br/>
     * For now only OAI and HTTP topologies can ignore the records in HarvestedRecordCategorizationBolt */
    private int ignoredRecordsCount;
    /** Number of records that was already processed by the topology that have 'deleted' flag on the revision*/
    private int deletedRecordsCount;
    private int processedErrorsCount;
    private int deletedErrorsCount;
    private int expectedPostProcessedRecordsNumber;
    private int postProcessedRecordsCount;
    /**Full definition of the task stored in Json format*/
    private String definition;
}
