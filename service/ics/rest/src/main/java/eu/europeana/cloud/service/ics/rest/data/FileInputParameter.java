package eu.europeana.cloud.service.ics.rest.data;

import java.util.List;

/**
 * Created by Tarek on 8/21/2015.
 */
public class FileInputParameter {
    private String cloudId;
    private String inputRepresentationName;
    private String inputExtension;
    private String providerId;
    private String inputVersion;
    private String fileName;
    private List<String> properties;
    private String outputRepresentationName;
    private String outputExtension;

    public String getInputVersion() {
        return inputVersion;
    }

    public void setInputVersion(String inputVersion) {
        this.inputVersion = inputVersion;
    }


    public String getCloudId() {
        return cloudId;
    }

    public void setCloudId(String cloudId) {
        this.cloudId = cloudId;
    }

    public String getInputRepresentationName() {
        return inputRepresentationName;
    }

    public void setInputRepresentationName(String inputRepresentationName) {
        this.inputRepresentationName = inputRepresentationName;
    }

    public String getInputExtension() {
        return inputExtension;
    }

    public void setInputExtension(String inputExtension) {
        this.inputExtension = inputExtension;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public List<String> getProperties() {
        return properties;
    }

    public void setProperties(List<String> properties) {
        this.properties = properties;
    }

    public String getOutputRepresentationName() {
        return outputRepresentationName;
    }

    public void setOutputRepresentationName(String outputRepresentationName) {
        this.outputRepresentationName = outputRepresentationName;
    }

    public String getOutputExtension() {
        return outputExtension;
    }

    public void setOutputExtension(String outputExtension) {
        this.outputExtension = outputExtension;
    }

    public String getProviderId() {
        return providerId;
    }

    public void setProviderId(String providerId) {
        this.providerId = providerId;
    }


}
