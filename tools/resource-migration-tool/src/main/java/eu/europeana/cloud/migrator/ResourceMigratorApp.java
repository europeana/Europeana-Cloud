package eu.europeana.cloud.migrator;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Properties;

public class ResourceMigratorApp {

    private static final Logger logger = Logger.getLogger(ResourceMigratorApp.class);

    public static void main(String[] args) {
        boolean clean = false;
        if (args.length > 0)
            clean = Boolean.valueOf(args[0]);

        boolean simulate = args.length == 1 && "simulate".equals(args[0]) || args.length == 2 && "simulate".equals(args[1]);

        ApplicationContext context = getContext();

        if (context == null) {
            logger.error("Spring configuration files not found!");
            System.exit(-1);
        }

        ResourceMigrator migrator = (ResourceMigrator) context.getBean("migrator");
        migrator.migrate(clean, simulate);
        System.exit(0);
    }

    private static Properties loadPropertiesFile(File dpFile) {
        Properties props = new Properties();
        InputStream is = null;
        try {
            is = new FileInputStream(dpFile);
            props.load(is);
        } catch (IOException e) {
            logger.error("Could not load properties file " + dpFile.getAbsolutePath());
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                logger.error("Could not close input stream.", e);
            }
        }
        return props;
    }

    private static ApplicationContext getContext() {
        Properties config = loadPropertiesFile(new File("config.properties"));
        if (config.isEmpty())
            return null;

        try {
            String content = new String(Files.readAllBytes(FileSystems.getDefault().getPath(".", "spring-config.xml")));
            for (Object obj : config.keySet()) {
                String value = (String) config.get(obj);
                if (value == null)
                    value = "";
                String key = "${" + obj + "}";
                content = content.replace(key, value);
            }
            Files.write(FileSystems.getDefault().getPath(".", "spring-config-configured.xml"), content.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        } catch (IOException e) {
            return null;
        }
        return new FileSystemXmlApplicationContext("spring-config-configured.xml");
    }
}
