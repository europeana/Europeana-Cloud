package eu.europeana.cloud.service.commons.logging;

import kafka.producer.KafkaLog4jAppender;
import org.apache.log4j.Appender;
import org.apache.log4j.EnhancedPatternLayout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import org.apache.log4j.PatternLayout;

import java.util.Enumeration;

public class LoggerUpdater {

    public void addKafkaAppender(Logger logger, String brokerList, String topicName, String applicationName, String hostname) {
        EnhancedPatternLayout layout = new EnhancedPatternLayout();
        layout.setConversionPattern("%d{yyyy-MM-dd'T'HH:mm:ss.SSS'Z'}{GMT} " + applicationName + " " + hostname + " [%C] - %p : %m");

        KafkaLog4jAppender kafkaAppender = new KafkaLog4jAppender();
        kafkaAppender.setThreshold(Level.INFO);
        kafkaAppender.setName("kafka.producer.KafkaLog4jAppender");
        kafkaAppender.setLayout(layout);
        kafkaAppender.setBrokerList(brokerList);
        kafkaAppender.setTopic(topicName);
        kafkaAppender.setProducerType("async");
        kafkaAppender.activateOptions();

        logger.addAppender(kafkaAppender);
    }

    /**
     * Replaces selected text to another in all appenders of selected logger
     *
     * @param logger        logger selected for update
     * @param textToReplace part of addender's pattern that will be replaced
     * @param newValue      replacement
     */
    public void update(Logger logger, String textToReplace, String newValue) {

        Enumeration<Appender> appenderEnumerator = logger.getAllAppenders();
        while (appenderEnumerator.hasMoreElements()) {
            Appender appender = appenderEnumerator.nextElement();
            updateAppender(appender, textToReplace, newValue);
        }
    }

    private void updateAppender(Appender appender, String textToReplace, String newValue) {
        EnhancedPatternLayout t = null;
        if (appender.getLayout() instanceof PatternLayout) {
            PatternLayout patternLayout = (PatternLayout) appender.getLayout();
            String pattern = patternLayout.getConversionPattern();
            patternLayout.setConversionPattern(patternLayout.getConversionPattern().replace(textToReplace, newValue));
        } else if (appender.getLayout() instanceof EnhancedPatternLayout) {
            EnhancedPatternLayout patternLayout = (EnhancedPatternLayout) appender.getLayout();
            String pattern = patternLayout.getConversionPattern();
            patternLayout.setConversionPattern(patternLayout.getConversionPattern().replace(textToReplace, newValue));
        }
    }
}
