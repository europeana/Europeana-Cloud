package eu.europeana.cloud.service.dps.storm.topologies.ic.converter.utlis;


import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.exceptions.ConversionException;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Executes a shell command
 */
public class CommandExecutor {
    private static final Logger LOGGER = Logger.getLogger(CommandExecutor.class);

    /**
     * Executes a shell command and logs the output message
     *
     * @param command The shell command to be executed
     */
    public void execute(String command) throws ConversionException, IOException {
        String output = "";
        if (command != null) {
            Process p;
            p = Runtime.getRuntime().exec(command);
            output = readOutPut(p.getErrorStream());
            if (!"".equals(output)) {
                throw new ConversionException(output);
            } else {
                output = readOutPut(p.getInputStream());
                LOGGER.info("The command was executed successfully with the following output message: " + output);
            }
            p.destroy();
        }
    }

    private String readOutPut(InputStream stream) throws IOException {
        StringBuffer output = new StringBuffer();
        BufferedReader reader = null;
        try {
            reader =
                    new BufferedReader(new InputStreamReader(stream));
            String line = "";
            while ((line = reader.readLine()) != null) {
                output.append(line + "\n");
            }
        } finally {
            if (reader != null)
                reader.close();
        }

        return output.toString();

    }

}
