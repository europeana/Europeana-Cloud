package eu.europeana.cloud.common.response;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * Rest response that is returned if some error occurred.
 */
@XmlRootElement
public class ErrorInfo {

	/**
	 * Code of error. This is specific for a particular rest api.
	 */
    private String errorCode;

	/**
	 * Details message for error.
	 */
    private String details;


    /**
     * Creates a new instance of this class.
     */
    public ErrorInfo() {
    }


    /**
     * Creates a new instance of this class.
     * @param errorCode
     * @param details
     */
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
