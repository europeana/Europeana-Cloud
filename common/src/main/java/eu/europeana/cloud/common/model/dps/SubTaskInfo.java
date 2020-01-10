package eu.europeana.cloud.common.model.dps;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement()
public class SubTaskInfo {
    private int resourceNum;
    private String resource;
    private RecordState recordState;
    private String info;
    private String additionalInformations;
    private String resultResource;

    public SubTaskInfo() {

    }

    public SubTaskInfo(int resourceNum, String resource, RecordState recordState, String info, String additionalInformations) {
        this.resource=resource;
        this.recordState = recordState;
        this.info = info;
        this.additionalInformations = additionalInformations;
        this.resourceNum = resourceNum;

    }

    public SubTaskInfo(int resourceNum, String resource, RecordState recordState, String info, String additionalInformations, String resultResource) {
        this(resourceNum, resource, recordState, info, additionalInformations);
        this.resultResource = resultResource;
    }

    public int getResourceNum() {
        return resourceNum;
    }

    public void setResourceNum(int resourceNum) {
        this.resourceNum = resourceNum;
    }

    public void setResource(String resource) {
        this.resource = resource;
    }

    public String getAdditionalInformations() {
        return additionalInformations;
    }

    public void setAdditionalInformations(String additionalInformations) {
        this.additionalInformations = additionalInformations;
    }

    public String getResultResource() {
        return resultResource;
    }

    public void setResultResource(String resultResource) {
        this.resultResource = resultResource;
    }

    public SubTaskInfo(String resource) {
        this.resource = resource;
    }

    public String getResource() {
        return resource;
    }

    public RecordState getRecordState() {
        return recordState;
    }

    public void setRecordState(RecordState recordState) {
        this.recordState = recordState;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SubTaskInfo)) {
            return false;
        }

        SubTaskInfo that = (SubTaskInfo) o;

        if (additionalInformations != null ? !additionalInformations.equals(that.additionalInformations) : that.additionalInformations != null) {
            return false;
        }
        if (info != null ? !info.equals(that.info) : that.info != null) {
            return false;
        }
        if (resource != null ? !resource.equals(that.resource) : that.resource != null) {
            return false;
        }

        if (resourceNum != that.resourceNum) {
            return false;
        }

        if (resultResource != null ? !resultResource.equals(that.resultResource) : that.resultResource != null) {
            return false;
        }
        if (recordState != that.recordState) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = resource != null ? resource.hashCode() : 0;
        result = 31 * result + (recordState != null ? recordState.hashCode() : 0);
        result = 31 * result + (info != null ? info.hashCode() : 0);
        result = 31 * result + (additionalInformations != null ? additionalInformations.hashCode() : 0);
        result = 31 * result + (resultResource != null ? resultResource.hashCode() : 0);
        return result;
    }
}
