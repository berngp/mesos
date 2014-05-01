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

public class Variable {
  protected Variable() {}

  /**
   * Returns the current value of this variable.
   * @return the value in bytes
   */
  public native byte[] value();

  /**
   * Updates the current value of this variable.
   *
   * @param value   The new value.
   *
   * @return A variable representing the new value.
   */
  public native Variable mutate(byte[] value);

  protected native void finalize();

  private long __variable;
}
