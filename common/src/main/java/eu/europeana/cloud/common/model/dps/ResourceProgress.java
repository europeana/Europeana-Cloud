package eu.europeana.cloud.common.model.dps;

/**
 * Created by Tarek on 8/7/2018.
 */
public class ResourceProgress {
    private long taskId;

    public ResourceProgress(long taskId, String resource, States state, String resultResource) {
        this.taskId = taskId;
        this.resource = resource;
        this.state = state;
        this.resultResource = resultResource;
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public String getResource() {
        return resource;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    public States getState() {
        return state;
    }

    public void setState(States state) {
        this.state = state;
    }

    public String getResultResource() {
        return resultResource;
    }

    public void setResultResource(String resultResource) {
        this.resultResource = resultResource;
    }

    private String resource;
    private States state;
    private String resultResource;

    public ResourceProgress()
    {

    }




}
