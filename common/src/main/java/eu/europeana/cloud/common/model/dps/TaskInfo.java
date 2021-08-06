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
    private int expectedRecordsNumber;
    private int processedRecordsCount;
    /** Number of records that was already processed by the topology that have 'deleted' flag on the revision*/
    private int deletedRecordsCount;
    /** Number of records that was already processed by the topology that was ignored by this execution.<br/>
     * For now only OAI and HTTP topologies can ignore the records in HarvestedRecordCategorizationBolt */
    private int ignoredRecordsCount;
    private TaskState state;
    private String stateDescription;
    /** Tomcat application identifier where the task was executed */
    private String ownerId;
    private Date sentTimestamp;
    private Date startTimestamp;
    private Date finishTimestamp;
    private int processedErrorsCount;
    private int deletedErrorsCount;
    private String topicName;
    /**Full definition of the task stored in Json format*/
    private String definition;
}
