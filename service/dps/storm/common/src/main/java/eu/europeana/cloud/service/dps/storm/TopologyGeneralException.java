package eu.europeana.cloud.service.dps.storm;

public class TopologyGeneralException extends RuntimeException {
    public TopologyGeneralException(String message) {
        super(message);
    }

    public TopologyGeneralException(String message, Throwable cause) {
        super(message, cause);
    }
}
