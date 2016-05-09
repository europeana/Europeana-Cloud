package eu.europeana.cloud.common.model.dps;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TaskInfo {
    private final long id;
    private final String topologyName;
    private int containsElements;
    private TaskState state;
    private String info;
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


    public TaskInfo(long id, String topologyName, TaskState state, String info) {
        this.id = id;
        this.topologyName = topologyName;
        this.state = state;
        this.info = info;

    }

    public TaskInfo(long id, String topologyName, TaskState state) {
        this(id, topologyName, state, "");

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

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + (topologyName != null ? topologyName.hashCode() : 0);
        result = 31 * result + containsElements;
        result = 31 * result + (subtasks != null ? subtasks.hashCode() : 0);
        return result;
    }
}
