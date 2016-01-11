package eu.europeana.cloud.migrator;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Properties;

public class ResourceMigratorApp {
    public static void main(String[] args) {
        boolean clean = false;
        if (args.length > 0)
            clean = Boolean.valueOf(args[0]);

        boolean builtIn = args.length == 1 && "in".equals(args[0]) || args.length == 2 && "in".equals(args[1]);

        ApplicationContext context = getContext(builtIn);

        if (context == null) {
            System.err.println("Configuration files not found!");
            System.exit(-1);
        }

        ResourceMigrator migrator = (ResourceMigrator) context.getBean("migrator");
        migrator.migrate(clean);
        System.exit(0);
    }

    private static Properties loadPropertiesFile(File dpFile) {
        Properties props = new Properties();
        InputStream is = null;
        try {
            is = new FileInputStream(dpFile);
            props.load(is);
        } catch (IOException e) {
            System.err.println("Problem with file " + dpFile.getAbsolutePath());
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                System.err.println("Could not close input stream.");
            }
        }
        return props;
    }

    private static ApplicationContext getContext(boolean builtIn) {
        if (builtIn)
            return new ClassPathXmlApplicationContext("spring-config.xml");

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
