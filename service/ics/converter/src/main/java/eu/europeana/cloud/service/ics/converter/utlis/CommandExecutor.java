package eu.europeana.cloud.service.ics.converter.utlis;


import org.apache.log4j.Logger;

import java.io.*;

/**
 * Executes a shell command
 */
public class CommandExecutor {
    private final static Logger LOGGER = Logger.getLogger(CommandExecutor.class);

    /**
     * Executes a shell command and logs the output message
     * @param command
     * The shell command to be executed
     */
    public void execute(String command) throws IOException{
        StringBuffer output = new StringBuffer();
        if (command != null) {
            Process p;

                p = Runtime.getRuntime().exec(command);
                BufferedReader reader =
                        new BufferedReader(new InputStreamReader(p.getInputStream()));
                String line = "";
                while ((line = reader.readLine()) != null) {
                    output.append(line + "\n");
                }
                LOGGER.info("The command was executed successfully with the following output message: " + output);


        }
    }

}
