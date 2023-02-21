package eu.europeana.cloud.common.utils;

/**
 * This is class to be used in common 'is this first time?' example.
 * <p>
 * Because it provides {@link #unpack()} method, the user of this class does not have to add <code>first = false</code> after
 * checking its value.
 */
public class FirstFlag {

  private boolean first = true;

  /**
   * Creates instance of FirstFlag.
   * <p>
   * Object is initialised in the way that first {@link #unpack()} method call will return <code>true</code> and next
   * {@link #unpack()} method calls will return <code>false</code>.
   */
  public FirstFlag() {

  }

  /**
   * Check if this is the first time.
   * <p>
   * If this is the first call of this method for this object it will return
   * <code>true</code>, subsequent calls will return <code>false</code>.
   *
   * @return value indicating whether this is the first call of this method or not
   */
  public boolean unpack() {
    if (!first) {
      return false;
    }
    first = false;
    return true;
  }

  /**
   * Resets flag to original state.
   * <p>
   * After calling this method, object returns to its original state. The next call of {@link #unpack()} method will return
   * <code>true</code> and subsequent will again return <code>false</code>.
   */
  public void reset() {
    first = true;
  }
}
