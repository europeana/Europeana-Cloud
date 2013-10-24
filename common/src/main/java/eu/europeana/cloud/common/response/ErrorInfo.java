package eu.europeana.cloud.common.response;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * ErrorInfo
 */
@XmlRootElement
public class ErrorInfo {

    private String errorCode;

    private String details;


    public ErrorInfo() {
    }


    public ErrorInfo(String errorCode, String details) {
        this.errorCode = errorCode;
        this.details = details;
    }


    public String getErrorCode() {
        return errorCode;
    }


    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }


    public String getDetails() {
        return details;
    }


    public void setDetails(String details) {
        this.details = details;
    }
}
