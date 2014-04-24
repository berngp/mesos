/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.mesos.state;

import java.util.Iterator;

import java.util.concurrent.Future;

/**
 * An abstraction of "state" (possibly between multiple distributed
 * components) represented by "variables" (effectively key/value
 * pairs). Variables are versioned such that setting a variable in the
 * state will only succeed if the variable has not changed since last
 * fetched. Varying implementations of state provide varying
 * replicated guarantees.
 *
 * Note that the semantics of 'fetch' and 'store' provide
 * atomicity. That is, you can not store a variable that has changed
 * since you did the last fetch. That is, if a store succeeds then no
 * other writes have been performed on the variable since your fetch.
 *
 * Example:
 * <pre>
 * {@code
 *   State state = new ZooKeeperState();
 *   Future<Variable> variable = state.fetch("machines");
 *   Variable machines = variable.get();
 *   machines = machines.mutate(...);
 *   variable = state.store(machines);
 *   machines = variable.get();
 * }
 * </pre>
 */
public interface State {
  /**
   * Returns an immutable "variable" representing the current value
   * from the state associated with the specified name.
   *
   * @param name    of the variable to fetch.
   * @return        a future that wraps the variable that is being fetched.
   * @see           Variable
   */
  Future<Variable> fetch(String name);

  /**
   * Returns an immutable "variable" representing the current value in
   * the state if updating the specified variable in the state was
   * successful, otherwise returns null.
   *
   * @param variable    variable to store.
   * @return            a future that wraps the variable that was stored.
   * @see               Variable
   */
  Future<Variable> store(Variable variable);

  /**
   * Returns true if successfully expunged the variable from the state
   * or false if the variable did not exist or was no longer valid.
   *
   * @param variable variable that is going to be expunged.
   * @return         a future that wraps the outcome of the expunge.
   */
  Future<Boolean> expunge(Variable variable);

  /**
   * Returns an iterator of variable names in the state.
   * @return the iterator of the names.
   */
  Future<Iterator<String>> names();
}
