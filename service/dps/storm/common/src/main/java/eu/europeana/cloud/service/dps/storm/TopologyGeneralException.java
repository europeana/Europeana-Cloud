package eu.europeana.cloud.service.dps.storm;

public class TopologyGeneralException extends RuntimeException {
    public TopologyGeneralException(String message) {
        super(message);
    }

    public TopologyGeneralException(Throwable throwable) {
        super(throwable);
    }

    public TopologyGeneralException(String message, Throwable cause) {
        super(message, cause);
    }
}
