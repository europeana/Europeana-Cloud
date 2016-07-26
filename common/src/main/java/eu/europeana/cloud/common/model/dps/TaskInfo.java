package eu.europeana.cloud.common.model.dps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class TaskInfo {
    private final long id;
    private final String topologyName;
    private int containsElements;
    private TaskState state;
    private String info;

    private Date finishDate;
    private Date startDate;
    private Date sentDate;
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


    public TaskInfo(long id, String topologyName, TaskState state, String info, Date sentDate, Date startDate, Date finishDate) {
        this.id = id;
        this.topologyName = topologyName;
        this.state = state;
        this.info = info;
        this.sentDate = sentDate;
        this.startDate = startDate;
        this.finishDate = finishDate;
    }


    public long getId() {
        return id;
    }

    public String getTopologyName() {
        return topologyName;
    }

    public int getContainsElements() {
        return containsElements;
    }

    public void setContainsElements(int containsElements) {
        this.containsElements = containsElements;
    }

    public List<SubTaskInfo> getSubtasks() {
        return Collections.unmodifiableList(subtasks);
    }

    public void addSubtask(SubTaskInfo subtask) {
        subtasks.add(subtask);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TaskInfo)) return false;

        TaskInfo taskInfo = (TaskInfo) o;

        if (containsElements != taskInfo.containsElements) return false;
        if (id != taskInfo.id) return false;
        if (subtasks != null ? !subtasks.equals(taskInfo.subtasks) : taskInfo.subtasks != null) return false;
        if (topologyName != null ? !topologyName.equals(taskInfo.topologyName) : taskInfo.topologyName != null)
            return false;
        if (state != taskInfo.state) return false;
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

        return true;
    }


    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + (topologyName != null ? topologyName.hashCode() : 0);
        result = 31 * result + containsElements;
        result = 31 * result + (subtasks != null ? subtasks.hashCode() : 0);
        result = 31 * result + (state != null ? state.hashCode() : 0);
        result = 31 * result + (startDate != null ? startDate.hashCode() : 0);
        result = 31 * result + (sentDate != null ? sentDate.hashCode() : 0);
        return result;
    }
}
