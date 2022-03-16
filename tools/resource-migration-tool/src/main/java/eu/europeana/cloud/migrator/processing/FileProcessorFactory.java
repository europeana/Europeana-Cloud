package eu.europeana.cloud.migrator.processing;

import java.lang.reflect.InvocationTargetException;

public class FileProcessorFactory {

    /** Class name of the processor object that should be created. */
    private String processingClass;

    /** Configuration file name for the processing class. */
    private String processingConfig;


    public FileProcessorFactory(String processingClass, String processingConfig) {
        this.processingClass = processingClass;
        this.processingConfig = processingConfig;
    }

    public FileProcessor create() {
        if (processingClass == null || processingClass.isEmpty())
            return null;

        Class<?> newClass = null;

        try {
            newClass = Class.forName(processingClass);
            return (FileProcessor) newClass.getConstructor(String.class).newInstance(processingConfig);
        }
        catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            return null;
        }
    }
}
