package eu.europeana.cloud.client.uis.rest.console;

/**
 * Command line application for Connection REST API
 * @author Yorgos.Mamamakis@ kb.nl
 *
 */
public class Client {

	private Client(){
		
	}
	
	public static void main(String[] args) {
		App app = new App();
		Thread t = new Thread(app);
		t.start();
	}

}
