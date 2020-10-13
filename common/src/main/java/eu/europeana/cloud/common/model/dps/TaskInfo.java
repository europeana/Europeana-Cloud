package eu.europeana.cloud.common.model.dps;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

@XmlRootElement()
public class TaskInfo {

    private static final int DEFAULT_PROGRESS_PERCENTAGE = -1;

    private long id;

    public void setId(long id) {
        this.id = id;
    }

    public void setTopologyName(String topologyName) {
        this.topologyName = topologyName;
    }

    private String topologyName;
    private int expectedSize;
    private int processedElementCount;
    private int retryCount;
    private TaskState state;
    private String info;
    private String ownerId;

    private Date finishDate;
    private Date startDate;
    private Date sentDate;

    private int processedPercentage;
    private int errors;

    private String taskDefinition;

    private String topicName;


    public TaskInfo() {

    }

    public void setTaskDefinition(String taskDefinition) {
        this.taskDefinition = taskDefinition;
    }

    public String getTaskDefinition() {
        return taskDefinition;
    }

    public Date getFinishDate() {
        return finishDate;
    }

    public void setFinishDate(Date finishDate) {
        this.finishDate = finishDate;
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public Date getSentDate() {
        return sentDate;
    }

    public void setSentDate(Date sentDate) {
        this.sentDate = sentDate;
    }

    public void setSubtasks(List<SubTaskInfo> subtasks) {
        this.subtasks = subtasks;
    }

    public int getProcessedElementCount() {
        return processedElementCount;
    }

    public void setProcessedElementCount(int processedElementCount) {
        this.processedElementCount = processedElementCount;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public int getProcessedPercentage() {
        return processedPercentage;
    }

    public void setProcessedPercentage(int processedPercentage) {
        this.processedPercentage = processedPercentage;
    }

    public int getErrors() {
        return errors;
    }

    public void setErrors(int errors) {
        this.errors = errors;
    }

    private List<SubTaskInfo> subtasks = new ArrayList<>();

    public TaskState getState() {
        return state;
    }

    public void setState(TaskState state) {
        this.state = state;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(String ownerId) {
        this.ownerId = ownerId;
    }

    private void calculateProgress() {
        if (expectedSize < 0) {
            processedPercentage = DEFAULT_PROGRESS_PERCENTAGE;
        } else {
            processedPercentage = expectedSize > 0 ? 100 * processedElementCount / expectedSize : 0;
        }
    }

    public TaskInfo(long id, String topologyName, TaskState state, String info, Date sentDate, Date startDate, Date finishDate) {
        this(id, topologyName, state, info, 0, 0, 0, 0, sentDate, startDate, finishDate);
    }

    public TaskInfo(long id, String topologyName, TaskState state, String info, int containsElements, int processedElementCount, int retryCount, int errors, Date sentDate, Date startDate, Date finishDate) {
        this.id = id;
        this.topologyName = topologyName;
        this.state = state;
        this.info = info;
        this.expectedSize = containsElements;
        this.processedElementCount = processedElementCount;
        this.retryCount = retryCount;
        this.sentDate = sentDate;
        this.startDate = startDate;
        this.finishDate = finishDate;
        this.errors = errors;
        calculateProgress();
    }


    public long getId() {
        return id;
    }

    public String getTopologyName() {
        return topologyName;
    }

    public int getExpectedSize() {
        return expectedSize;
    }

    public void setExpectedSize(int expectedSize) {
        this.expectedSize = expectedSize;
    }

    public List<SubTaskInfo> getSubtasks() {
        return Collections.unmodifiableList(subtasks);
    }

    public void addSubtask(SubTaskInfo subtask) {
        subtasks.add(subtask);
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o){
            return true;
        }
        if (!(o instanceof TaskInfo)){
            return false;
        }

        TaskInfo taskInfo = (TaskInfo) o;

        if (expectedSize != taskInfo.expectedSize){
            return false;
        }
        if (errors != taskInfo.errors){
            return false;
        }
        if (processedPercentage != taskInfo.processedPercentage){
            return false;
        }
        if (id != taskInfo.id){
            return false;
        }
        if (subtasks != null ? !subtasks.equals(taskInfo.subtasks) : taskInfo.subtasks != null){
            return false;
        }
        if (topologyName != null ? !topologyName.equals(taskInfo.topologyName) : taskInfo.topologyName != null){
            return false;
        }

        if (state != taskInfo.state){
            return false;
        }
        if (startDate == null)
            if (taskInfo.startDate != null) return false;
        if (startDate != null && taskInfo.startDate != null)
            if (startDate.getTime() != taskInfo.startDate.getTime()) return false;
        if (sentDate == null)
            if (taskInfo.sentDate != null) return false;
        if (sentDate != null && taskInfo.sentDate != null)
            if (sentDate.getTime() != taskInfo.sentDate.getTime()) return false;
        if (finishDate == null)
            if (taskInfo.finishDate != null) return false;
        if (finishDate != null && taskInfo.finishDate != null)
            if (finishDate.getTime() != taskInfo.finishDate.getTime()) return false;
        if (ownerId == null)
            if (taskInfo.ownerId != null) return false;
        if (ownerId != null && taskInfo.ownerId == null)
            return false;
        if (ownerId != null)
            if (!ownerId.equals(taskInfo.ownerId)) return false;
        if (topicName != null ? !topicName.equals(taskInfo.topicName) : taskInfo.topicName != null)
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + (topologyName != null ? topologyName.hashCode() : 0);
        result = 31 * result + expectedSize;
        result = 31 * result + errors;
        result = 31 * result + processedPercentage;
        result = 31 * result + (subtasks != null ? subtasks.hashCode() : 0);
        result = 31 * result + (state != null ? state.hashCode() : 0);
        result = 31 * result + (startDate != null ? startDate.hashCode() : 0);
        result = 31 * result + (sentDate != null ? sentDate.hashCode() : 0);
        result = 31 * result + (ownerId != null ? ownerId.hashCode() : 0);
        result = 31 * result + (topicName != null ? topicName.hashCode() : 0);
        return result;
    }
}
