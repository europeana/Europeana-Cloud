package eu.europeana.cloud.database.truncate;

import org.springframework.context.ApplicationContext;

import eu.europeana.cloud.database.truncate.context.ApplicationContextUtils;

/**
 * Truncate application 
 * [Usage: java -cp ecloud-service-uis-truncate-0.1-SNAPSHOT.jar eu.europeana.cloud.database.truncate.TruncateApp]
 * 
 * 
 * @author Yorgos Mamakis (Yorgos.Mamakis@ europeana.eu)
 * @since Jan 10, 2014
 */
public class TruncateApp 
{
	
	/**
	 * 
	 * @param args
	 */
    public static void main( String[] args )
    {
        ApplicationContext context = ApplicationContextUtils.getApplicationContext();
        Truncator truncator =(Truncator) context.getBean("truncator");
        truncator.truncate();
    }
    
    
}
