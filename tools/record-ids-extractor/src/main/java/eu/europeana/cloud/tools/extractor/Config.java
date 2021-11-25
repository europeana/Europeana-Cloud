package eu.europeana.cloud.tools.extractor;

import org.apache.commons.configuration2.ImmutableConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;

public class Config {

    private static final ImmutableConfiguration CONFIGURATION;

    static {
        try {
            CONFIGURATION = new FileBasedConfigurationBuilder<>(PropertiesConfiguration.class)
                    .configure(new Parameters().properties()
                            .setFileName("config.properties").setThrowExceptionOnMissing(true)).getConfiguration();
        } catch (ConfigurationException e) {
            throw new ConfigFileLoadException("Could not load configuration file! Valid configuration file named config.properties - based on config.properties.template should be present in program root directory.", e);
        }
    }


    public static final String UIS_URL = CONFIGURATION.getString("uis.url");
    public static final String MCS_URL = CONFIGURATION.getString("mcs.url");
    public static final String ECLOUD_USER = CONFIGURATION.getString("ecloud.user");
    public static final String ECLOUD_PASSWORD = CONFIGURATION.getString("ecloud.password");
    public static final String DATASET_PROVIDER = CONFIGURATION.getString("dataset.provider");
    public static final String REVISION_PROVIDER = CONFIGURATION.getString("revision.provider");
    public static final String REPRESENTATION_NAME = CONFIGURATION.getString("representation.name");

    private Config() {
    }
}
