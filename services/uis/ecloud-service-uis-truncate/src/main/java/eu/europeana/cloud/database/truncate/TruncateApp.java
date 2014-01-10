package eu.europeana.cloud.database.truncate;

import org.springframework.context.ApplicationContext;

import eu.europeana.cloud.database.truncate.context.ApplicationContextUtils;

public class TruncateApp 
{
	
	
    public static void main( String[] args )
    {
        ApplicationContext context = ApplicationContextUtils.getApplicationContext();
        Truncator truncator =(Truncator) context.getBean("truncator");
        truncator.truncate();
    }
    
    
}
