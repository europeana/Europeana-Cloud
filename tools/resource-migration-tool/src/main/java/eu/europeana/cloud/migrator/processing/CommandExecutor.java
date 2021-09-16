package eu.europeana.cloud.migrator.processing;

import eu.europeana.cloud.migrator.ResourceMigratorApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CommandExecutor {

    private static final String WORKING_DIR_PARAM = "command.working.dir";

    private static final String COMMAND_PARAM = "command";

    private static final int BUFFER_SIZE = 1024;

    private List<String> commandTemplates = new ArrayList<String>();

    protected String workingDirectory;

    private static final Logger logger = LoggerFactory.getLogger(CommandExecutor.class);

    public CommandExecutor(String configFile) {
        loadConfig(ResourceMigratorApp.loadPropertiesFile(new File(configFile)));
    }


    private void loadConfig(Properties props)
    {
        workingDirectory = props.getProperty(WORKING_DIR_PARAM);
        if (workingDirectory == null || workingDirectory.isEmpty())
            workingDirectory = System.getProperty("java.io.tmpdir");

        String command = null;
        int counter = 0;

        while (true) {
            command = props.getProperty(COMMAND_PARAM + "." + String.valueOf(++counter));
            if (command == null)
                break;

            commandTemplates.add(command);
        }
    }


    protected String prepareCommand(String command, List<String> parameters)
    {
        for (int i = 0; i < parameters.size(); i++) {
            command = command.replace("$" + (i + 1), parameters.get(i));
        }
        return command;
    }


    protected CommandResult runCommand(int index, List<String> parameters)
    {
        String command = getCommand(index);
        command = prepareCommand(command, parameters);
        if (command == null) {
            CommandResult cr = new CommandResult();
            cr.setResult(false);
            return cr;
        }

        return runCommand(command);
    }


    private String getCommand(int index)
    {
        if (index < 0 || index >= commandTemplates.size())
            return null;

        return commandTemplates.get(index);
    }


    protected CommandResult runCommand(String command)
    {
        CommandResult result = new CommandResult();

        if (logger.isDebugEnabled())
            logger.debug("Starting execution of command: {}", command);

        try {
            Process process = Runtime.getRuntime().exec(command);

            // start a separate thread for reading the stderr
            final InputStream stderr = process.getErrorStream();
            new Thread("CmdExecutor-stderrReader") {

                public void run()
                {
                    try {
                        readAndLog(stderr);
                    }
                    catch (IOException e) {
                        logger.error("Error getting process output", e);
                    }
                }


                // read the stderr stream and log the problems
                private void readAndLog(InputStream in)
                        throws IOException
                {
                    byte[] buffer = new byte[BUFFER_SIZE];
                    int read, offset = 0;
                    while ((read = in
                            .read(buffer, offset, BUFFER_SIZE - offset)) != -1) {
                        offset += read;
                        if (read < 0 || in.available() < 1
                                || offset >= BUFFER_SIZE) {
                            if (offset > 0) {
                                logger.warn(new String(buffer, 0, offset));
                            }
                            offset = 0;
                        }
                    }
                }
            }.start();

            // read the stdout stream in the main thread
            String out = read(process.getInputStream());
            if (!out.isEmpty() && logger.isDebugEnabled()) {
                logger.debug("Command output: {}", out);
            }
            result.setStdOut(out);

            // wait for the process to finish and check the status
            int exitStatus;
            exitStatus = process.waitFor();
            if (exitStatus != 0) {
                logger.warn("Command executed unsuccessfully with exit status {}: {}", exitStatus, command);
                result.setResult(false);
                result.setExitStatus(exitStatus);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Command execution interrupted: " + command, e);
        }
        catch (IOException e) {
            logger.warn("Command execution failed: " + command, e);
        }

        if (logger.isDebugEnabled()) {
            if (result.getResult())
                logger.debug("Command: {} executed successfully.", command);
            else
                logger.debug("Command: {} executed with errors.", command);
        }

        return result;
    }


    private String read(InputStream in)
            throws IOException
    {
        byte[] buffer = new byte[BUFFER_SIZE];
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        int read;
        while ((read = in.read(buffer, 0, BUFFER_SIZE)) != -1) {
            os.write(buffer, 0, read);
        }
        return os.toString();
    }
}

