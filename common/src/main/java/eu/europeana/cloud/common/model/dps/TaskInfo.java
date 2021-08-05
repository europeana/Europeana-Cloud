package eu.europeana.cloud.common.model.dps;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Date;

@XmlRootElement()
@Builder
@Data
@AllArgsConstructor
public class TaskInfo {

    private long id;
    private String topologyName;
    private int expectedRecordsNumber;
    private int processedRecordsCount;
    /** Number of records that was already processed by the topology that have 'deleted' flag on the revision*/
    private int deletedRecordsCount;
    /** Number of records that was already processed by the topology that was ignored by this execution.<br/>
     * For now only OAI topology can ignore the records in HarvestedRecordCategorizationBolt */
    private int ignoredRecordsCount;
    /** Number of retries done on the records processed by the topology*/
    private int retryCount;
    private TaskState state;
    private String stateDescription;
    /** Tomcat application identifier where the task was executed */
    private String ownerId;
    private Date finishDate;
    private Date startDate;
    private Date sentDate;
    private int processedErrorsCount;
    private int deletedErrorsCount;
    private String topicName;
    /**Full definition of the task stored in Json format*/
    private String definition;
}
