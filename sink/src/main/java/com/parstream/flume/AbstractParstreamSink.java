/**
 * Copyright 2015 ParStream GmbH
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
package com.parstream.flume;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import com.google.common.base.Preconditions;
import com.parstream.driver.ColumnInfo;
import com.parstream.driver.ParstreamConnection;
import com.parstream.driver.ParstreamException;
import com.parstream.driver.ParstreamFatalException;

public abstract class AbstractParstreamSink extends AbstractSink implements Configurable {

    private static final Log LOGGER = LogFactory.getLog(AbstractParstreamSink.class);

    private static final String CONF_HOST = "parstreamServerAddress";
    private static final String CONF_PORT = "parstreamServerPort";
    private static final String CONF_USERNAME = "parstreamServerUsername";
    private static final String CONF_PASSWORD = "parstreamServerPassword";
    private static final String CONF_TABLE_NAME = "parstreamImportTable";
    private static final String CONF_TIMEOUT = "parstreamServerTimeout";
    private static final String CONF_COMMIT_RATE = "parstreamCommitRate";
    private static final String CONF_FATAL_ATTEMPT = "parstreamFatalExceptionAttempts";
    protected static final String CONF_AVRO_INI = "parstreamAvroConfPath";
    protected static final String CONF_JSON_INI = "parstreamJsonConfPath";

    private String _parstreamServer;
    private String _parstreamPort;
    private String _parstreamUsername;
    private String _parstreamPassword;
    private String _parstreamTableName;

    private Integer _parstreamTimeout;
    private Integer _parstreamCommitRate;
    private Integer _parstreamFatalAttempts;
    private Integer _batchSize;

    private ParstreamConnection _parstreamConn;
    private int _pendingRowCount;
    private boolean _insertPrepared;

    private ColumnInfo[] _columnInfos;

    private ParstreamSinkCounter _sinkCounter;

    protected abstract void processEvent(final Event event);

    /**
     * Additional checks that might be added to a customized sink implementation
     * 
     * @param context
     */
    protected abstract void checkPreconditions(final Context context);

    @Override
    public void start() {
        _sinkCounter.start();

        // connect to ParStream, obtain column information
        restartParstreamConnection();
    }

    /**
     * The following is a description of the parameters expected in the flume
     * configuration file (scope: ParstreamSink). All are obligatory unless
     * stated otherwise
     * 
     * parstreamServerAddress: the address where the Parstream server resides
     * 
     * parstreamServerPort: the server port of the Parstream server
     * 
     * parstreamServerUsername: (optional, default: null): Parstream username
     * 
     * parstreamServerPassword: (optional, default: null): Parstream password
     * 
     * parstreamServerTimeout: import connection timeout in milliseconds
     * 
     * parstreamImportTable: table name to which data will be imported
     * 
     * parstreamCommitRate: number of rows per single commit
     * 
     * parstreamFatalExceptionAttempts (optional, default: 3): the number of
     * successive attempts to restart a Parstream connection incase a fatal
     * exception occurs. Afterwards a FlumeException is thrown
     */
    @Override
    public void configure(final Context context) {
        _parstreamServer = context.getString(CONF_HOST);
        _parstreamPort = context.getString(CONF_PORT);
        _parstreamUsername = context.getString(CONF_USERNAME);
        _parstreamPassword = context.getString(CONF_PASSWORD);
        _parstreamTimeout = context.getInteger(CONF_TIMEOUT);
        _parstreamTableName = context.getString(CONF_TABLE_NAME);
        _parstreamCommitRate = context.getInteger(CONF_COMMIT_RATE);
        _parstreamFatalAttempts = context.getInteger(CONF_FATAL_ATTEMPT, 3);

        _batchSize = context.getInteger("batch-size", 1);

        if (_sinkCounter == null) {
            _sinkCounter = new ParstreamSinkCounter(getName());
        }

        // first make a check on the common configuration of any sink
        checkPreconditions();

        // then make a check on the specific conditions of the implementing sink
        checkPreconditions(context);
    }

    private void checkPreconditions() {
        Preconditions.checkState(_parstreamServer != null,
                "No Parstream server host specified. Use configuration key: " + CONF_HOST);

        Preconditions.checkState(_parstreamPort != null, "No Parstream server port specified. Use configuration key: "
                + CONF_PORT);

        Preconditions.checkState(_parstreamTimeout != null,
                "No Parstream connection timeout specified. Use configuration key: " + CONF_TIMEOUT);

        Preconditions.checkState(_parstreamTableName != null,
                "No Parstream table name specified. Use configuration key: " + CONF_TABLE_NAME);

        Preconditions.checkState(_parstreamCommitRate != null,
                "No Parstream commit rate specified. Use configuration key: " + CONF_COMMIT_RATE);

        Preconditions.checkState(_parstreamCommitRate > 0, "Parstream commit rate must be bigger than 0.");

        isParstreamConnectable();
    }

    private void isParstreamConnectable() throws IllegalStateException {
        ParstreamConnection conn = new ParstreamConnection();
        try {
            conn.createHandle();
            conn.setTimeout(_parstreamTimeout);
            conn.connect(_parstreamServer, _parstreamPort, _parstreamUsername, _parstreamPassword);
            conn.listImportColumns(_parstreamTableName);
        } catch (ParstreamFatalException e) {
            throw new IllegalStateException("Unable to establish a connection with Parstream.");
        } catch (ParstreamException e) {
            // import table does not exist
            throw new IllegalStateException(
                    "Configured Parstream table name does not exist in the provided Parstream server.");
        } finally {
            conn.close();
        }
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try {
            List<Event> batch = new ArrayList<Event>(getBatchSize());
            for (int i = 0; i < getBatchSize(); ++i) {
                getSinkCounter().incrementEventDrainAttemptCount();
                Event event = channel.take();
                getSinkCounter().incrementEventDrainSuccessCount();

                if (event != null) {
                    batch.add(event);
                } else {
                    getSinkCounter().incrementBatchUnderflowCount();
                    commit();
                }
            }

            if (batch.isEmpty()) {
                getSinkCounter().incrementBatchEmptyCount();
                status = Status.BACKOFF;
            } else {
                for (Event e : batch) {
                    processEvent(e);
                }
            }

            transaction.commit();
        } catch (ChannelException e) {
            transaction.rollback();
            logError(LOGGER, "Unable to get event from channel. Exception follows.", e);
            status = Status.BACKOFF;
        } finally {
            transaction.close();
        }

        return status;
    }

    @Override
    public void stop() {
        if (_parstreamConn != null) {
            commit();
            _parstreamConn.close();
            _sinkCounter.incrementConnectionClosedCount();
        }
        _sinkCounter.stop();
    }

    /**
     * Restarts the ParStream connection
     */
    private void restartParstreamConnection() {
        _insertPrepared = false;
        _pendingRowCount = 0;
        if (_parstreamConn != null) {
            _parstreamConn.close();
        }
        try {
            _parstreamConn = new ParstreamConnection();
            _parstreamConn.createHandle();
            _parstreamConn.setTimeout(_parstreamTimeout);
            _parstreamConn.connect(_parstreamServer, _parstreamPort, _parstreamUsername, _parstreamPassword);
            _sinkCounter.incrementConnectionCreatedCount();

            _columnInfos = _parstreamConn.listImportColumns(_parstreamTableName);
        } catch (ParstreamFatalException e) {
            if (_parstreamConn != null) {
                _parstreamConn.close();
            }
            throw new IllegalStateException(e.getMessage());
        } catch (ParstreamException e) {
            logError(LOGGER, e.getMessage(), e);
        }
    }

    /**
     * Inserts a list of rows into ParStream.
     * 
     * @param rowsToInsert
     *            a list of rows to be inserted into ParStream
     */
    protected void insertIntoParstream(final List<Object[]> rowsToInsert) {
        boolean insertedWithoutErrors = false;
        int attemptNo = 1;
        while (attemptNo < _parstreamFatalAttempts && !insertedWithoutErrors) {
            try {
                ++attemptNo;
                insertAttempt(rowsToInsert);
                insertedWithoutErrors = true;
            } catch (ParstreamFatalException e) {
                logError(LOGGER, e.getMessage(), e);
                restartParstreamConnection();
            }
        }

        if (!insertedWithoutErrors) {
            throw new IllegalStateException(
                    "Parstream encountered "
                            + attemptNo
                            + " successive ParstreamFatalException. Something looks really wrong with the dataset attempted for insertion.");
        }
    }

    private void insertAttempt(final List<Object[]> rowsToInsert) throws ParstreamFatalException {
        for (Object[] row : rowsToInsert) {
            // Ensure the parstream connection is prepared for insertion
            if (!_insertPrepared) {
                try {
                    _parstreamConn.prepareInsert(_parstreamTableName);
                    _insertPrepared = true;
                    _pendingRowCount = 0;
                } catch (ParstreamException e) {
                    // nothing to do. This exception was already taken care of
                    // during the configuration of this sink
                }
            }

            try {
                _parstreamConn.rawInsert(row);
                ++_pendingRowCount;
            } catch (ParstreamException e) {
                StringBuilder strBuilder = new StringBuilder();
                for (Object colVal : row) {
                    strBuilder.append(colVal).append(";");
                }
                String errorMessage = e.getMessage() + " Attempted row data to insert: " + strBuilder.toString();
                logError(LOGGER, errorMessage, e);
                _sinkCounter.incrementParstreamRejectedRows();
            } catch (ParstreamFatalException e) {
                logError(LOGGER, e.getMessage(), e);
                _sinkCounter.incrementParstreamRejectedRows();
                restartParstreamConnection();
                throw e;
            }

            if (_insertPrepared && _pendingRowCount >= _parstreamCommitRate) {
                commit();
            }
        }
    }

    /**
     * Commits data into ParStream in case the connection was already prepared
     * for insertion.
     */
    protected void commit() {
        if (_insertPrepared) {
            try {
                logInfo(LOGGER, "will commit " + _pendingRowCount + " rows to ParStream");
                _sinkCounter.incrementParstreamCommitAttempt();
                _parstreamConn.commit();
                _sinkCounter.incrementParstreamCommitSuccessful();
                _sinkCounter.addToParstreamCommittedRows(_pendingRowCount);
                logInfo(LOGGER, "finished commiting " + _pendingRowCount + " rows to ParStream");
                _pendingRowCount = 0;
                _insertPrepared = false;
            } catch (ParstreamFatalException e) {
                logError(LOGGER, e.getMessage(), e);
            }
        }
    }

    protected int getBatchSize() {
        return _batchSize;
    }

    protected ColumnInfo[] getColumnInfo() {
        return _columnInfos;
    }

    protected ParstreamSinkCounter getSinkCounter() {
        return _sinkCounter;
    }

    protected void logError(Log log, String msg, Exception e) {
        log.error("Sink " + getName() + ": " + msg, e);
    }

    protected void logInfo(Log log, String msg) {
        if (log.isInfoEnabled()) {
            log.info("Sink " + getName() + ": " + msg);
        }
    }
}
