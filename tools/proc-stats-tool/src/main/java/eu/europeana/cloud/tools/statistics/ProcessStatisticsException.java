package eu.europeana.cloud.tools.statistics;

public class ProcessStatisticsException extends RuntimeException {
    public ProcessStatisticsException(String message) {
        super(message);
    }

    @SuppressWarnings("unused")
    public ProcessStatisticsException(Throwable cause) {
        super(cause);
    }

    @SuppressWarnings("unused")
    public ProcessStatisticsException(String message, Throwable cause) {
        super(message, cause);
    }
}
