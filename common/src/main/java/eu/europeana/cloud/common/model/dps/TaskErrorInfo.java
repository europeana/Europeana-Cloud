package eu.europeana.cloud.common.model.dps;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement()
public class TaskErrorInfo {
    private String errorType;

    private String message;

    private int occurrences;

    private List<String> identifiers;


    public TaskErrorInfo() {

    }

    public TaskErrorInfo(String errorType, String message, int occurrences) {
        this(errorType, message, occurrences, null);
    }

    public TaskErrorInfo(String errorType, String message, int occurrences, List<String> identifiers) {
        this.errorType = errorType;
        this.message = message;
        this.occurrences = occurrences;
        if (identifiers == null) {
            this.identifiers = new ArrayList<>();
        } else {
            this.identifiers = identifiers;
        }
    }


    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public int getOccurrences() {
        return occurrences;
    }

    public void setOccurrences(int occurrences) {
        this.occurrences = occurrences;
    }

    public List<String> getIdentifiers() {
        return identifiers;
    }

    public void setIdentifiers(List<String> identifiers) {
        this.identifiers = identifiers;
    }

    public String getErrorType() {
        return errorType;
    }

    public void setErrorType(String errorType) {
        this.errorType = errorType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TaskErrorInfo)) {
            return false;
        }


        TaskErrorInfo taskInfo = (TaskErrorInfo) o;

        if (occurrences != taskInfo.occurrences) {
            return false;
        }
        if (message != null ? !message.equals(taskInfo.message) : taskInfo.message != null) {
            return false;
        }
        if (errorType != null ? !errorType.equals(taskInfo.errorType) : taskInfo.errorType != null) {
            return false;
        }
        if (identifiers != null ? !identifiers.equals(taskInfo.identifiers) : taskInfo.identifiers != null) {
            return false;
        }

        return true;
    }


    @Override
    public int hashCode() {
        int result = errorType != null ? errorType.hashCode() : 0;
        result = 31 * result + (message != null ? message.hashCode() : 0);
        result = 31 * result + occurrences;
        result = 31 * result + (identifiers != null ? identifiers.hashCode() : 0);
        return result;
    }
}
