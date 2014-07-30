package eu.europeana.cloud.service.aas.authentication;


/**
 * Provides core information for every user interacting with the ecloud
 * 
 * (username, password..)
 * 
 * @author emmanouil.koufakis@theeuropeanlibrary.org
 *
 */
public class UserImpl implements User {
	
	private final String username;
	
	private final String password;

	/**
	 * Provides core information for every user interacting with the ecloud
	 * 
	 * (username, password..)
	 */
	public UserImpl(final String username, final String password) {
		
		this.username = username;
		this.password = password;
	}

	@Override
	public String getPassword() {
		return password;
	}

	@Override
	public String getUsername() {
		return username;
	}
}
