package eu.europeana.cloud.service.aas.authentication;

/**
 * Provides core information for every user interacting with the ecloud
 * 
 * (username, password..)
 * 
 * @author emmanouil.koufakis@theeuropeanlibrary.org
 *
 */
public interface User {

	String getUsername();
	
	String getPassword();
}
