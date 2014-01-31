package eu.europeana.cloud.client.uis.rest.console;

import java.util.Scanner;

/**
 * Command line application for Connection REST API
 * 
 * @author Yorgos.Mamamakis@ kb.nl
 * 
 */
public final class Client {

	private Client() {

	}

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		Scanner scanner;
		while (true) {
			scanner = new Scanner(System.in);
			String[] input = scanner.nextLine().split(" ");
			if ("exit".equalsIgnoreCase(input[0])) {
				scanner.close();
				break;

			}
			for (int i = 0; i < Integer.parseInt(input[1]); i++) {
				App app = new App();
				String[] arguments = new String[input.length - 1];
				arguments[0] = input[0];
				int k = 2;
				while (k < input.length) {
					arguments[k - 1] = input[k];
					k++;
				}

				app.setInput(arguments);
				app.setId(i);
				Thread t = new Thread(app);
				t.start();
				
			}
			System.out.println("\n");

		}
	}

}
