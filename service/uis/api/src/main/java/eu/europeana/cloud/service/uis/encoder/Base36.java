package eu.europeana.cloud.service.uis.encoder;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Base36 encoder. The characters consists of consonants (capital only) and
 * numbers only. The implementation ensures that the same input will generate
 * the same result and that the output will always be 11 characters by filling
 * with 0
 * 
 * @href http://blog.maxant.co.uk/pebble/2010/02/02/1265138340000.html
 * @author Yorgos.Mamakis@ kb.nl
 * @since Oct 31, 2013
 */
public final class Base36 {

	private static final char[] DICTIONARY = new char[] { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'B', 'C',
			'D', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'V', 'W', 'X', 'Z' };

	private static final int ASCII_OFFSET = 48;
	private static final int OFFSET = 100;
	private static final int LENGTH = 11;

	private Base36() {

	}

	/**
	 * Encode a given string according to a custom Base36 implementation
	 * 
	 * @param str
	 *            The string to encode
	 * @return A 11 character encoded version of the String representation
	 */
	public static String encode(String str) {

		return encode(new BigInteger(convertToNum(str)));

	}

	/**
	 * Timestamp based randomization
	 * 
	 * @param str
	 *            The string to encode
	 * 
	 * @return A 11 character encoded version of the String representation
	 */
	public static String timeEncode(String str) {
		return encode(new BigInteger(convertToNum(new Date().getTime())));

	}

	private static String encode(BigInteger given) {
		List<Character> result = new ArrayList<>();
		BigInteger base = new BigInteger("" + DICTIONARY.length);
		int exponent = 1;
		BigInteger remaining = given;
		while (true) {
			BigInteger power = base.pow(exponent);
			BigInteger modulo = remaining.mod(power);
			BigInteger powerMinusOne = base.pow(exponent - 1);
			BigInteger times = modulo.divide(powerMinusOne);
			result.add(DICTIONARY[times.intValue()]);
			remaining = remaining.subtract(modulo);
			if (remaining.equals(BigInteger.ZERO)) {
				break;
			}
			exponent++;
		}
		StringBuilder sb = new StringBuilder();
		for (int i = result.size() - 1; i > -1; i--) {
			sb.append(result.get(i));
		}
		if (sb.length() < LENGTH) {
			char[] cArr = new char[11 - sb.length()];
			Arrays.fill(cArr, (char) ASCII_OFFSET);
			sb.append(cArr);
			return sb.reverse().toString();
		}
		return sb.substring(0, LENGTH);
	}

	private static String convertToNum(String str) {
		StringBuilder sb = new StringBuilder();
		for (char c : str.toCharArray()) {
			sb.append((c * ((int) (Math.random() * OFFSET))));
		}
		return sb.toString();
	}

	private static String convertToNum(long lng) {
		StringBuilder sb = new StringBuilder();
		for (char c : Long.toString(lng).toCharArray()) {
			sb.append((c * ((int) (Math.random() * OFFSET))));
		}
		return sb.toString();

	}
}
