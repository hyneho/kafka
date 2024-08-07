/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.raft;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.raft.internals.ReplicaKey;

import org.slf4j.Logger;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

/**
 * A replica is "unattached" when it doesn't know the leader or the leader's endpoint.
 *
 * Typically, a replica doesn't know the leader if the KRaft topic is undergoing an election cycle.
 *
 * It is also possible for a replica to be unattached if it doesn't know the leader's endpoint.
 * This typically happens when a replica starts up and the known leader id is not part of the local
 * voter set. In that case, during startup the replica transitions to unattached instead of
 * transitioning to follower. The unattached replica discovers the leader and leader's endpoint
 * either through random Fetch requests to the bootstrap servers or through BeginQuorumEpoch
 * request from the leader.
 */
// (non-voter) fetch from bootstrap servers until local finds leader
// voter will sit idle until a timeout and transition to candidate

public class UnattachedState implements EpochState {
    private final Time time;
    private int epoch;
    private OptionalInt leaderId;
//    private Optional<ReplicaKey> votedKey;
    private Optional<VotedState> votedState;
    private Set<Integer> voters;
    private long electionTimeoutMs;
    private Timer electionTimer;
    private final Optional<LogOffsetMetadata> highWatermark;
    private final Logger log;

    public UnattachedState(
        Time time,
        int epoch,
        OptionalInt leaderId,
        Optional<ReplicaKey> votedKey,
        Set<Integer> voters,
        Optional<LogOffsetMetadata> highWatermark,
        long electionTimeoutMs,
        LogContext logContext
    ) {
        this.time = time;
        this.epoch = epoch;
        this.leaderId = leaderId;
//        this.votedKey = votedKey;
        this.votedState = votedKey.map(VotedState::new);
        this.voters = voters;
        this.highWatermark = highWatermark;
        this.electionTimeoutMs = electionTimeoutMs;
        this.electionTimer = time.timer(electionTimeoutMs);
        this.log = logContext.logger(UnattachedState.class);
    }

    public void updateAfterVoting(int epoch, ReplicaKey votedKey, Set<Integer> voters, long electionTimeoutMs) {
        this.epoch = epoch;
//        this.votedKey = Optional.of(votedKey);
        this.votedState = Optional.of(new VotedState(votedKey));
        this.leaderId = OptionalInt.empty();
        this.voters = voters;
        this.electionTimeoutMs = electionTimeoutMs;
        this.electionTimer = time.timer(electionTimeoutMs);
    }

    @Override
    public ElectionState election() {
        if (votedState.isPresent()) {
            return votedState.get().election();
//        if (votedKey.isPresent()) {
//            return ElectionState.withVotedCandidate(epoch, votedKey.get(), voters);
        } else if (leaderId.isPresent()) {
            return ElectionState.withElectedLeader(epoch, leaderId.getAsInt(), voters);
        } else {
            return ElectionState.withUnknownLeader(epoch, voters);
        }
    }

    @Override
    public int epoch() {
        return epoch;
    }

    @Override
    public Endpoints leaderEndpoints() {
        return Endpoints.empty();
    }

    @Override
    public String name() {
        return "Unattached";
    }

    public Optional<ReplicaKey> votedKey() {
        return votedState.map(VotedState::votedKey);
    }

    public long electionTimeoutMs() {
        return electionTimeoutMs;
    }

    public long remainingElectionTimeMs(long currentTimeMs) {
        electionTimer.update(currentTimeMs);
        return electionTimer.remainingMs();
    }

    public boolean hasElectionTimeoutExpired(long currentTimeMs) {
        electionTimer.update(currentTimeMs);
        return electionTimer.isExpired();
    }

    @Override
    public Optional<LogOffsetMetadata> highWatermark() {
        return highWatermark;
    }

    @Override
    public boolean canGrantVote(ReplicaKey candidateKey, boolean isLogUpToDate) {
        if (votedState.isPresent()) {
            return votedState.get().canGrantVote(candidateKey);
//        if (votedKey.isPresent()) {
//            ReplicaKey votedReplicaKey = votedKey.get();
//            if (votedReplicaKey.id() == candidateKey.id()) {
//                return !votedReplicaKey.directoryId().isPresent() ||
//                    votedReplicaKey.directoryId().equals(candidateKey.directoryId());
//            }
//            log.debug(
//                "Rejecting vote request from candidate ({}), already have voted for another " +
//                    "candidate ({}) in epoch {}",
//                candidateKey,
//                votedReplicaKey,
//                epoch
//            );
//            return false;
        } else if (leaderId.isPresent()) {
            // If the leader id known it should behave similar to the follower state
            log.debug(
                "Rejecting vote request from candidate ({}) since we already have a leader {} in epoch {}",
                candidateKey,
                leaderId,
                epoch
            );
            return false;
        } else if (!isLogUpToDate) {
            log.debug(
                "Rejecting vote request from candidate ({}) since candidate epoch/offset is not up to date with us",
                candidateKey
            );
        }

        return isLogUpToDate;
    }

    @Override
    public String toString() {
        return "Unattached(" +
            "epoch=" + epoch +
            ", votedKey=" + (votedKey().isPresent() ? votedKey().get() : "null") +
            ", voters=" + voters +
            ", electionTimeoutMs=" + electionTimeoutMs +
            ", highWatermark=" + highWatermark +
            ')';
    }

    @Override
    public void close() {}

    private class VotedState {
        final ReplicaKey votedKey;

        VotedState(ReplicaKey votedKey) {
            this.votedKey = Objects.requireNonNull(votedKey);
        }

        ReplicaKey votedKey() {
            return votedKey;
        }

        ElectionState election() {
            return ElectionState.withVotedCandidate(epoch, votedKey, voters);
        }

        boolean canGrantVote(ReplicaKey candidateKey) {
            if (votedKey.id() == candidateKey.id()) {
                return !votedKey.directoryId().isPresent() || votedKey.directoryId().equals(candidateKey.directoryId());
            }

            log.debug(
                "Rejecting vote request from candidate ({}), already have voted for another " +
                "candidate ({}) in epoch {}",
                candidateKey,
                votedKey,
                epoch
            );

            return false;
        }
    }
}
