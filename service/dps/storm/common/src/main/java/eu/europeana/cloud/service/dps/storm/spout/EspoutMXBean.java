package eu.europeana.cloud.service.dps.storm.spout;

public interface EspoutMXBean {
    String showSpoutToString();

    String showOffsetManagers();

    String showEmitted() ;

    String getLastConsumedMessageId();

    String getLastConsumedMessage();

    String getLastAckedMessageId();

    String getLastFailedMessageId();
}
