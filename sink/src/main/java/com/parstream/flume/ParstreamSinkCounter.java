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

import org.apache.flume.instrumentation.MonitoredCounterGroup;

public class ParstreamSinkCounter extends MonitoredCounterGroup implements ParstreamSinkCounterMBean {

    private static final String COUNTER_BATCH_EMPTY = "sink.batch.empty";
    private static final String COUNTER_BATCH_UNDERFLOW = "sink.batch.underflow";
    private static final String COUNTER_EVENT_DRAIN_ATTEMPT = "sink.event.drain.attempt";
    private static final String COUNTER_EVENT_DRAIN_SUCCESS = "sink.event.drain.sucess";

    private static final String COUNTER_CONNECTION_CREATED = "sink.parstream.connection.creation.count";
    private static final String COUNTER_CONNECTION_CLOSED = "sink.parstream.connection.closed.count";

    private static final String COUNTER_PARSTREAM_COMMIT_ATTEMPT = "sink.parstream.commit.attempt.count";
    private static final String COUNTER_PARSTREAM_COMMIT_SUCCESSFUL = "sink.parstream.commit.successful";
    private static final String COUNTER_PARSTREAM_COMMITTED_ROWS = "sink.parstream.commit.rows.count";
    private static final String COUNTER_PARSTREAM_REJECTED_ROWS = "sink.parstream.commit.rows.reject";

    private static final String[] ATTRIBUTES = { COUNTER_CONNECTION_CREATED, COUNTER_CONNECTION_CLOSED,
            COUNTER_BATCH_EMPTY, COUNTER_BATCH_UNDERFLOW, COUNTER_EVENT_DRAIN_ATTEMPT, COUNTER_EVENT_DRAIN_SUCCESS,
            COUNTER_PARSTREAM_COMMIT_ATTEMPT, COUNTER_PARSTREAM_COMMIT_SUCCESSFUL, COUNTER_PARSTREAM_COMMITTED_ROWS,
            COUNTER_PARSTREAM_REJECTED_ROWS };

    public ParstreamSinkCounter(String name) {
        super(MonitoredCounterGroup.Type.SINK, name, ATTRIBUTES);
    }

    @Override
    public long getConnectionCreatedCount() {
        return get(COUNTER_CONNECTION_CREATED);
    }

    public long incrementConnectionCreatedCount() {
        return increment(COUNTER_CONNECTION_CREATED);
    }

    @Override
    public long getConnectionClosedCount() {
        return get(COUNTER_CONNECTION_CLOSED);
    }

    public long incrementConnectionClosedCount() {
        return increment(COUNTER_CONNECTION_CLOSED);
    }

    @Override
    public long getBatchEmptyCount() {
        return get(COUNTER_BATCH_EMPTY);
    }

    public long incrementBatchEmptyCount() {
        return increment(COUNTER_BATCH_EMPTY);
    }

    @Override
    public long getBatchUnderflowCount() {
        return get(COUNTER_BATCH_UNDERFLOW);
    }

    public long incrementBatchUnderflowCount() {
        return increment(COUNTER_BATCH_UNDERFLOW);
    }

    @Override
    public long getEventDrainAttemptCount() {
        return get(COUNTER_EVENT_DRAIN_ATTEMPT);
    }

    public long incrementEventDrainAttemptCount() {
        return increment(COUNTER_EVENT_DRAIN_ATTEMPT);
    }

    @Override
    public long getEventDrainSuccessCount() {
        return get(COUNTER_EVENT_DRAIN_SUCCESS);
    }

    public long incrementEventDrainSuccessCount() {
        return increment(COUNTER_EVENT_DRAIN_SUCCESS);
    }

    @Override
    public long getParstreamCommitAttempt() {
        return get(COUNTER_PARSTREAM_COMMIT_ATTEMPT);
    }

    public long incrementParstreamCommitAttempt() {
        return increment(COUNTER_PARSTREAM_COMMIT_ATTEMPT);
    }

    @Override
    public long getParstreamCommitSuccessful() {
        return get(COUNTER_PARSTREAM_COMMIT_SUCCESSFUL);
    }

    public long incrementParstreamCommitSuccessful() {
        return increment(COUNTER_PARSTREAM_COMMIT_SUCCESSFUL);
    }

    @Override
    public long getParstreamCommittedRows() {
        return get(COUNTER_PARSTREAM_COMMITTED_ROWS);
    }

    public long addToParstreamCommittedRows(long delta) {
        return addAndGet(COUNTER_PARSTREAM_COMMITTED_ROWS, delta);
    }

    @Override
    public long getParstreamRejectedRows() {
        return get(COUNTER_PARSTREAM_REJECTED_ROWS);
    }

    public long incrementParstreamRejectedRows() {
        return increment(COUNTER_PARSTREAM_REJECTED_ROWS);
    }
}
