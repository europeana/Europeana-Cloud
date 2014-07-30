package eu.europeana.cloud.service.aas.authentication;

/**
 * Used to create / retrieve / update <code>User</code> objects from ecloud's database. 
 * 
 * @author emmanouil.koufakis@theeuropeanlibrary.org
 * 
 */
public interface UserDAO {

	User getUser(final String username);
}
