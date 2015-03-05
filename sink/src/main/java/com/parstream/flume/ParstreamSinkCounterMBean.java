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

public interface ParstreamSinkCounterMBean {

    long getConnectionCreatedCount();

    long getConnectionClosedCount();

    long getBatchEmptyCount();

    long getBatchUnderflowCount();

    long getEventDrainAttemptCount();

    long getEventDrainSuccessCount();

    long getStartTime();

    long getStopTime();

    String getType();

    long getParstreamCommitAttempt();

    long getParstreamCommitSuccessful();

    long getParstreamCommittedRows();

    long getParstreamRejectedRows();
}
