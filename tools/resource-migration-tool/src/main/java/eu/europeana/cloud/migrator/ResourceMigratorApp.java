package eu.europeana.cloud.migrator;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.FileSystemXmlApplicationContext;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;

public class ResourceMigratorApp {

    private static final Logger logger = Logger.getLogger(ResourceMigratorApp.class);

    private static final String CLEAN = "clean";

    private static final String SIMULATE = "simulate";

    private static final String VERIFY = "verify";

    private static final String VERIFYLOCAL = "verifylocal";

    private static final String GRANT_PUBLIC_ACCESS = "grant";

    private static ArgumentParser prepareArgumentParser() {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("ResourceMigratorApp")
                .defaultHelp(true)
                .description("Migrate files from specified location to Europeana Cloud.");
        parser.addArgument("-c", "--clean").type(Boolean.TYPE).action(Arguments.storeTrue()).help("Clean previously added records.");
        parser.addArgument("-path").help("Only in Windows.");
        parser.addArgument("-g", "--grant").type(Boolean.TYPE).action(Arguments.storeTrue()).help("Grant .");

        MutuallyExclusiveGroup group = parser.addMutuallyExclusiveGroup();
        group.addArgument("-v", "--verify").type(Boolean.TYPE).action(Arguments.storeTrue()).help("Verify the migration from files point of view.");
        group.addArgument("-V", "--verifylocal").type(Boolean.TYPE).action(Arguments.storeTrue()).help("Verify whether all local identifiers given in a mapping file were migrated. NOTE: works for selected resource providers only.");
        group.addArgument("-s", "--simulate").type(Boolean.TYPE).action(Arguments.storeTrue()).help("Simulate migration to get the names of the files affected by the migration.");

        return parser;
    }

    public static void main(String[] args) {
        ArgumentParser parser = prepareArgumentParser();
        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }

        ApplicationContext context = getContext();

        if (context == null) {
            logger.error("Spring configuration files not found!");
            System.exit(-1);
        }

        ResourceMigrator migrator = (ResourceMigrator) context.getBean("migrator");
        if (ns.getBoolean(VERIFYLOCAL))
            migrator.verifyLocalIds();
        else if (ns.getBoolean(VERIFY))
            migrator.verify();
        else if (ns.getBoolean(GRANT_PUBLIC_ACCESS))
            migrator.grant(ns.getBoolean(SIMULATE));
        else
            migrator.migrate(ns.getBoolean(CLEAN), ns.getBoolean(SIMULATE));
        System.exit(0);
    }

    public static Properties loadPropertiesFile(File dpFile) {
        Properties props = new Properties();
        InputStream is = null;
        try {
            is = new FileInputStream(dpFile);
            props.load(is);
        } catch (IOException e) {
            logger.error("Could not load properties file " + dpFile.getAbsolutePath());
        } finally {
            try {
                if (is != null)
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
            
            Iterator<Entry<Object, Object>> iterator = config.entrySet().iterator();
            while(iterator.hasNext()) {
            	Entry<Object, Object> entry = iterator.next();
            	
                String value = (String)entry.getValue();
                if (value == null)
                    value = "";
                String key = "${" + entry.getKey() + "}";
                content = content.replace(key, value);
            }
            
            Files.write(FileSystems.getDefault().getPath(".", "spring-config-configured.xml"), content.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        } catch (IOException e) {
            return null;
        }
        return new FileSystemXmlApplicationContext("spring-config-configured.xml");
    }
}
