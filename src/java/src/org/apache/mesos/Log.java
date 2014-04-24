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

package org.apache.mesos;

import java.io.Closeable;
import java.io.IOException;

import java.util.List;
import java.util.Set;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

/**
 * Provides access to a distributed append only log. The log can be
 * read from using a {@link Log.Reader} and written to using a {@link Log.Writer}.
 */
public class Log {
  static {
    MesosNativeLibrary.load();
  }

  /**
   * An opaque identifier of a log entry's position within the
   * log. Can be used to inidicate {@link Log.Reader#read read} ranges and
   * {@link Log.Writer#truncate truncation} locations.
   */
  public static class Position implements Comparable<Position> {
    @Override
    public int compareTo(Position that) {
      return Long.signum(value - that.value);
    }

    @Override
    public boolean equals(Object that) {
      return that instanceof Position && value == ((Position) that).value;
    }

    @Override
    public String toString() {
      return "Position " + value;
    }

    /**
     * Returns an "identity" of this position, useful for serializing
     * to logs or across communication mediums.
     *
     * @return the identity
     */
    public byte[] identity() {
      byte[] bytes = new byte[8];
      bytes[0] = (byte) (0xff & (value >> 56));
      bytes[1] = (byte) (0xff & (value >> 48));
      bytes[2] = (byte) (0xff & (value >> 40));
      bytes[3] = (byte) (0xff & (value >> 32));
      bytes[4] = (byte) (0xff & (value >> 24));
      bytes[5] = (byte) (0xff & (value >> 16));
      bytes[6] = (byte) (0xff & (value >> 8));
      bytes[7] = (byte) (0xff & value);
      return bytes;
    }

    /* A Position is (and should only be) invoked by the underlying JNI. */
    private Position(long value) {
      this.value = value;
    }

    private final long value;
  }

  /**
   * Represents an opaque data entry in the {@link Log} with a {@link Log.Position}.
   */
  public static class Entry {
    /**
     * The position of this entry.
     * @see Position
     */
    public final Position position;
    /** The data at the given position.*/
    public final byte[] data;

    /* An Entry is (and should only be) invoked by the underlying JNI. */
    private Entry(Position position, byte[] data) {
      this.position = position;
      this.data = data;
    }
  }

  /**
   * An exception that gets thrown when an error occurs while
   * performing a read or write operation.
   */
  public static class OperationFailedException extends Exception {
    /**
     * @param message the message for this exception.
     */
    public OperationFailedException(String message) {
      super(message);
    }

    /**
     * @param message   the message for this exception.
     * @param cause     the underlying reason this exception was generated.
     */
    public OperationFailedException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * An exception that gets thrown when a writer no longer has the
   * ability to perform operations (e.g., because it was superseded by
   * another writer).
   */
  public static class WriterFailedException extends Exception {
    /**
     * @param message the message for this exception.
     */
    public WriterFailedException(String message) {
      super(message);
    }

    /**
     * @param message   the message for this exception.
     * @param cause     the underlying reason this exception was generated.
     */
    public WriterFailedException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * Provides read access to the {@link Log}. This class is safe for
   * use from multiple threads and for the life of the log regardless
   * of any exceptions thrown from its methods.
   */
  public static class Reader {
    /**
     * Returns an instance of a reader that will access the given instance of the Log.
     * @param log the log that this reader will access.
     */
    public Reader(Log log) {
      this.log = log;
      initialize(log);
    }

    /**
     * Attempts to read from the log between the specified positions
     * (inclusive). If either of the positions are invalid, an
     * OperationFailedException will get thrown. Unfortunately, this
     * will also get thrown in other circumstances (e.g., disk
     * failure) and therefore it is currently impossible to tell these
     * two cases apart.
     *
     * @param from      where to start reading.
     * @param to        where to finish reading.
     * @param timeout   max number of time units to wait before a {@link TimeoutException}.
     * @param unit      type of units used for the timeout, e.g. seconds, minutes, etc.
     *
     * @return list of entries fetched from the Log.
     *
     * @throws TimeoutException         if the read doesn't happen before the timeout.
     * @throws OperationFailedException if the read fails due that the read no longer has the ability
     *                                  to perform its operations.
     * @see Position
     * @see TimeUnit
     */
    public native List<Entry> read(Position from,
                                   Position to,
                                   long timeout,
                                   TimeUnit unit)
      throws TimeoutException, OperationFailedException;

    /**
     * Returns the beginning position of the log (might be out of date
     * with respect to another replica).
     * @return the beginning position of the log.
     */
    public native Position beginning();

    /**
     * Returns the ending position of the log (might be out of date
     * with respect to another replica).
     * @return the ending position of the log
     */
    public native Position ending();

    protected native void initialize(Log log);

    protected native void finalize();

    private Log log; // Keeps the log from getting garbage collected.
    private long __log;
    private long __reader;
  }

  /**
   * Provides write access to the {@link Log}. This class is not safe
   * for use from multiple threads and instances should be thrown out
   * after any {@link WriterFailedException} is thrown.
   */
  public static class Writer {
    /**
     * Constructs a writer linked the given {@link Log}.
     *
     * @param log       the log that this writer will access.
     * @param timeout   max number of time units to wait before a {@link TimeoutException}.
     * @param unit      type of time units used for the timeout, e.g. seconds, minutes, etc.
     * @param retries   number of retries
     *
     * @see TimeUnit
     */
    public Writer(Log log, long timeout, TimeUnit unit, int retries) {
      this.log = log;
      initialize(log, timeout, unit, retries);
    }

    /**
     * Attempts to append to the log with the specified data returning
     * the new end position of the log if successful.
     *
     * @param data      data to append to the log.
     * @param timeout   max number of time units to wait before a {@link TimeoutException}.
     * @param unit      type of time units used for the timeout, e.g. seconds, minutes, etc.
     *
     * @throws TimeoutException         if the append doesn't happen before the timeout.
     * @throws WriterFailedException    if the append fails due that the writer no longer has the ability
     *                                  to perform its operations (e.g., because it was superseded by
     *                                  another writer).
     * @return the new end-position.
     *
     * @see TimeUnit
     * @see WriterFailedException
     */
    public native Position append(byte[] data, long timeout, TimeUnit unit)
      throws TimeoutException, WriterFailedException;

    /**
     * Attempts to truncate the log (from the beginning to the
     * specified position exclusive) If the position is invalid, an
     * {@link WriterFailedException} will get thrown. Unfortunately, this will
     * also get thrown in other circumstances (e.g., disk failure) and
     * therefore it is currently impossible to tell these two cases
     * apart.
     *
     * <p>TODO(benh): Throw both OperationFailedException and
     * WriterFailedException to differentiate the need for a new
     * writer from a bad position, or a bad disk, etc.
     *
     * @param to        the log will be truncated up to this point.
     * @param timeout   max number of time units to wait before a {@link TimeoutException}.
     * @param unit      type of time units used for the timeout, e.g. seconds, minutes, etc.
     *
     * @return the position after the truncation.
     *
     * @throws TimeoutException         if the truncation doesn't happen before the timeout.
     * @throws WriterFailedException    if the truncation fails due an invalid position or if
     *                                  the writer no longer has the ability to perform its
     *                                  operations (e.g., because it was superseded by another writer).
     */
    public native Position truncate(Position to, long timeout, TimeUnit unit)
      throws TimeoutException, WriterFailedException;

    protected native void initialize(Log log,
                                     long timeout,
                                     TimeUnit unit,
                                     int retries);

    protected native void finalize();

    private Log log; // Keeps the log from getting garbage collected.
    private long __log;
    private long __writer;
  }

  /**
   * Creates a new replicated log that assumes the specified quorum
   * size, is backed by a file at the specified path, and coordiantes
   * with other replicas via the set of process PIDs.
   *
   * @param quorum  the quorum size.
   * @param path    path to the file backing this log.
   * @param pids    PIDs of the replicas to coordinate with.
   */
  public Log(int quorum,
             String path,
             Set<String> pids) {
    initialize(quorum, path, pids);
  }

  /**
   * Creates a new replicated log that assumes the specified quorum
   * size, is backed by a file at the specified path, and coordiantes
   * with other replicas associated with the specified ZooKeeper
   * servers, timeout, and znode (or Zookeeper name space).
   *
   * @param quorum  the quorum size.
   * @param path    path to the file backing this log.
   * @param servers Zookeper servers/connection string.
   * @param timeout max number of time units to wait before a {@link TimeoutException}.
   * @param unit    type of time units used for the timeout, e.g. seconds, minutes, etc.
   * @param znode   the Zookeeper name space.
   */
  public Log(int quorum,
             String path,
             String servers,
             long timeout,
             TimeUnit unit,
             String znode) {
    initialize(quorum, path, servers, timeout, unit, znode);
  }

  /**
   * Creates a new replicated log that assumes the specified quorum
   * size, is backed by a file at the specified path, and coordiantes
   * with other replicas associated with the specified ZooKeeper
   * servers, timeout, and znode (or Zookeeper name space).
   *
   * @param quorum      the quorum size.
   * @param path        path to the file backing this log.
   * @param servers     Zookeper servers/connection string.
   * @param timeout     max number of time units to wait before a {@link TimeoutException}.
   * @param unit        type of time units used for the timeout, e.g. seconds, minutes, etc.
   * @param znode       the Zookeeper name space.
   * @param scheme      the authentication scheme to use
   * @param credentials encoded credentials given the authentication scheme.
   */
  public Log(int quorum,
             String path,
             String servers,
             long timeout,
             TimeUnit unit,
             String znode,
             String scheme,
             byte[] credentials) {
    initialize(quorum, path, servers, timeout, unit, znode, scheme, credentials);
  }

  /**
   * Returns a position based off of the bytes recovered from
   * Position.identity().
   *
   * @param identity    identity, in bytes, of the position.
   * @return the position
   */
  public Position position(byte[] identity) {
    long value =
      ((long) (identity[0] & 0xff) << 56) |
      ((long) (identity[1] & 0xff) << 48) |
      ((long) (identity[2] & 0xff) << 40) |
      ((long) (identity[3] & 0xff) << 32) |
      ((long) (identity[4] & 0xff) << 24) |
      ((long) (identity[5] & 0xff) << 16) |
      ((long) (identity[6] & 0xff) << 8) |
      ((long) (identity[7] & 0xff));
    return new Position(value);
  }

  protected native void initialize(int quorum,
                                   String path,
                                   Set<String> pids);

  protected native void initialize(int quorum,
                                   String path,
                                   String servers,
                                   long timeout,
                                   TimeUnit unit,
                                   String znode);

  protected native void initialize(int quorum,
                                   String path,
                                   String servers,
                                   long timeout,
                                   TimeUnit unit,
                                   String znode,
                                   String scheme,
                                   byte[] credentials);

  protected native void finalize();

  private long __log;
}
