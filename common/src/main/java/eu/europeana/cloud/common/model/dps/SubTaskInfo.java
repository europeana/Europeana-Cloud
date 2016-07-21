package eu.europeana.cloud.common.model.dps;

public class SubTaskInfo {


    private int resourceNum;
    private final String resource;
    private States state;
    private String info;
    private String additionalInformations;
    private String resultResource;

    public SubTaskInfo(int resourceNum, String resource, States state, String info, String additionalInformations) {
        this(resource);
        this.state = state;
        this.info = info;
        this.additionalInformations = additionalInformations;
        this.resourceNum = resourceNum;

    }

    public SubTaskInfo(int resourceNum, String resource, States state, String info, String additionalInformations, String resultResource) {
        this(resourceNum, resource, state, info, additionalInformations);
        this.resultResource = resultResource;
    }

    public SubTaskInfo(String resource) {
        this.resource = resource;
    }

    public String getResource() {
        return resource;
    }

    public States getState() {
        return state;
    }

    public void setState(States state) {
        this.state = state;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SubTaskInfo)) return false;

        SubTaskInfo that = (SubTaskInfo) o;

        if (additionalInformations != null ? !additionalInformations.equals(that.additionalInformations) : that.additionalInformations != null)
            return false;
        if (info != null ? !info.equals(that.info) : that.info != null) return false;
        if (resource != null ? !resource.equals(that.resource) : that.resource != null) return false;

        if (resourceNum != that.resourceNum) return false;

        if (resultResource != null ? !resultResource.equals(that.resultResource) : that.resultResource != null)
            return false;
        if (state != that.state) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = resource != null ? resource.hashCode() : 0;
        result = 31 * result + (state != null ? state.hashCode() : 0);
        result = 31 * result + (info != null ? info.hashCode() : 0);
        result = 31 * result + (additionalInformations != null ? additionalInformations.hashCode() : 0);
        result = 31 * result + (resultResource != null ? resultResource.hashCode() : 0);
        return result;
    }
}
