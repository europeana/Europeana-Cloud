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
package eu.europeana.aas.authorization.repository;

import eu.europeana.aas.authorization.model.AclEntry;
import eu.europeana.aas.authorization.model.AclObjectIdentity;
import eu.europeana.aas.authorization.repository.exceptions.AclNotFoundException;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Rigas Grigoropoulos
 */
public interface AclRepository {

  /**
   * Loads the {@link AclEntry} instances that apply for the passed {@link AclObjectIdentity} objects. The returned map is keyed
   * on the passed objects, with the values being sets of the applicable {@link AclEntry} instances, sorted by the order
   * parameter. Any unknown objects will not have a map key.
   *
   * @param objectIdsToLookup the objects to find {@link AclEntry} information for.
   * @return a map with a set of {@link AclEntry} instances for each {@link AclObjectIdentity} passed as an argument.
   */
  Map<AclObjectIdentity, Set<AclEntry>> findAcls(List<AclObjectIdentity> objectIdsToLookup);

  /**
   * Loads a fully populated {@link AclObjectIdentity} object from the database for the provided {@link AclObjectIdentity}. The
   * provided object must contain 'id' and 'objectClass' information.
   *
   * @param objectId the {@link AclObjectIdentity} to load from the database.
   * @return a fully populated {@link AclObjectIdentity} object.
   */
  AclObjectIdentity findAclObjectIdentity(AclObjectIdentity objectId);

  /**
   * Loads the {@link AclObjectIdentity} instances that use the specified parent.
   *
   * @param objectId the object to find children for.
   * @return the list of children.
   */
  List<AclObjectIdentity> findAclObjectIdentityChildren(AclObjectIdentity objectId);

  /**
   * Removes all relevant records for the provided {@link AclObjectIdentity} instances.
   *
   * @param objectIdsToDelete the {@link AclObjectIdentity} instances representing the records to delete.
   */
  void deleteAcls(List<AclObjectIdentity> objectIdsToDelete);

  /**
   * Saves an {@link AclObjectIdentity} record in the database.
   *
   * @param aoi the {@link AclObjectIdentity} to save.
   */
  void saveAcl(AclObjectIdentity aoi);

  /**
   * Changes an existing {@link AclObjectIdentity} or the related {@link AclEntry} records in the database.
   *
   * @param aoi the {@link AclObjectIdentity} to update.
   * @param entries the list of {@link AclEntry} objects to update.
   * @throws AclNotFoundException if the relevant record could not be found.
   */
  void updateAcl(AclObjectIdentity aoi, List<AclEntry> entries) throws AclNotFoundException;

}
