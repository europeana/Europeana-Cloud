package eu.europeana.cloud.utils;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Toolkit {
    static final Logger LOGGER = Logger.getLogger(Toolkit.class);

    public static List<String> readIdentifiers(String textFilename) {
        List<String> result = new ArrayList<>();

        File file = new File(textFilename);
        if(!file.exists() || !file.isFile()) {
            LOGGER.info(String.format("File '%s' doesn't exists or isn't an ordinary file", textFilename));
            return result;
        }

        try(LineNumberReader reader = new LineNumberReader(new FileReader(file))) {
            String line = null;
            do {
                line = reader.readLine();

                if(line != null) {
                    int commentIndex = line.indexOf('#');
                    if(commentIndex != -1) {
                        line = line.substring(0, commentIndex).trim();
                    } else {
                        line = line.trim();
                    }

                    if(!line.isEmpty()) {
                        result.add(line);
                    }
                }
            } while(line != null);


        } catch(IOException ioException) {
            LOGGER.error("Error while reading file with identifiers", ioException);
        }

        return result;
    }

}
