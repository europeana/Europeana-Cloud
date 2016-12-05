package eu.europeana.cloud.migrator.processing;

public class CommandResult {

    private String stdOut;

    private String errorOut;

    private int exitStatus;

    private boolean result;


    public CommandResult()
    {
        stdOut = "";
        errorOut = "";
        exitStatus = 0;
        result = true;
    }


    public String getStdOut()
    {
        return stdOut;
    }


    public void setStdOut(String stdOut)
    {
        this.stdOut = stdOut;
    }


    public String getErrorOut()
    {
        return errorOut;
    }


    public void setErrorOut(String errorOut)
    {
        this.errorOut = errorOut;
    }


    public int getExitStatus()
    {
        return exitStatus;
    }


    public void setExitStatus(int exitStatus)
    {
        this.exitStatus = exitStatus;
    }


    public boolean getResult()
    {
        return result;
    }


    public void setResult(boolean result)
    {
        this.result = result;
    }
}
