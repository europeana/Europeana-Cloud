/* Copyright 2013 Rigas Grigoropoulos
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package eu.europeana.aas.authorization.repository.exceptions;

/**
 * Thrown if an <code>AclObjectIdentity</code> cannot be found for the object.
 *
 * @author Rigas Grigoropoulos
 */
public class AclNotFoundException extends RuntimeException {

  private static final long serialVersionUID = 1891804328079992377L;

  /**
   * Constructs a new <code>AclNotFoundException</code> with the specified detail message.
   *
   * @param message the detail message. The detail message is saved for later retrieval by the {@link #getMessage()} method.
   */
  public AclNotFoundException(String message) {
    super(message);
  }

}
