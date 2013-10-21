package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * ErrorInfo
 */
@XmlRootElement
public class ErrorInfo {

    private String code;

    private String details;


    public ErrorInfo() {
    }


    public ErrorInfo(Exception e) {
        this.code = e.getClass().getSimpleName();
        this.details = e.getMessage();
    }


    public String getCode() {
        return code;
    }


    public void setCode(String code) {
        this.code = code;
    }


    public String getDetails() {
        return details;
    }


    public void setDetails(String details) {
        this.details = details;
    }
}
