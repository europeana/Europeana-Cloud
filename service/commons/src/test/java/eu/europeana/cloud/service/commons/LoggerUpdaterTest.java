package eu.europeana.cloud.service.commons;
        
import eu.europeana.cloud.service.commons.logging.LoggerUpdater;
import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.EnhancedPatternLayout;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.Priority;
import org.apache.log4j.spi.LoggerFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Enumeration;

public class LoggerUpdaterTest {
    
    private Logger wellFormedLogger;
    private Logger loggerWithoutAppenders;
    
    private static final String PATTERN = "%d{yyyy-MM-dd'T'HH:mm:ss.SSS'Z'}{GMT} mcs instanceName - %p : %m";
    private static final String TEXT_TO_REPLACE = "instanceName";
    private static final String NEW_TEXT_VALUE = "sampleInstance";
    
    @Before
    public void prepareTestLoggers(){
        prepareWellFormedLogger();
        prepareLoggerWithoutAppenders();
    }
    
    @Test
    public void testWellFormedLogger(){
        LoggerUpdater loggerUpdater = new LoggerUpdater();
        loggerUpdater.update(wellFormedLogger, TEXT_TO_REPLACE, NEW_TEXT_VALUE);
        assertLogger(wellFormedLogger);
    }
    
    @Test
    public void testLoggerWithoutAppenders(){
        LoggerUpdater loggerUpdater = new LoggerUpdater();
        loggerUpdater.update(loggerWithoutAppenders, TEXT_TO_REPLACE, NEW_TEXT_VALUE);
        assertLogger(loggerWithoutAppenders);
    }
   
    private void assertLogger(Logger logger){
        Enumeration<Appender> appenderEnumerator = logger.getAllAppenders();
        while (appenderEnumerator.hasMoreElements()) {
            Appender appender = appenderEnumerator.nextElement();
            if (appender.getLayout() instanceof PatternLayout) {
                PatternLayout patternLayout = (PatternLayout) appender.getLayout();
                Assert.assertTrue(patternLayout.getConversionPattern().contains(NEW_TEXT_VALUE));
            } else if (appender.getLayout() instanceof EnhancedPatternLayout) {
                EnhancedPatternLayout patternLayout = (EnhancedPatternLayout) appender.getLayout();
                Assert.assertTrue(patternLayout.getConversionPattern().contains(NEW_TEXT_VALUE));
            }
        }
    }
    
    private void prepareWellFormedLogger(){
        wellFormedLogger = Logger.getLogger("sampleTestLogger");
        wellFormedLogger.addAppender(createConsoleAppender());
        wellFormedLogger.addAppender(createFileAppender());
    }
    
    private void prepareLoggerWithoutAppenders(){
        loggerWithoutAppenders = Logger.getLogger("sampleTestLoggerWithoutAppenders");
    }
    
    private Appender createConsoleAppender(){
        ConsoleAppender consoleAppender = new ConsoleAppender();
        consoleAppender.setLayout(new EnhancedPatternLayout(PATTERN));
        consoleAppender.setThreshold(Level.INFO);
        consoleAppender.activateOptions();
        return consoleAppender;
    }
    
    private Appender createFileAppender(){
        FileAppender fa = new FileAppender();
        fa.setName("FileLogger");
        fa.setFile("mylog.log");
        fa.setLayout(new PatternLayout(PATTERN));
        fa.setThreshold(Level.DEBUG);
        fa.setAppend(true);
        fa.activateOptions();
        return fa;
    }
}