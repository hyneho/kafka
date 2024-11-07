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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.FetchSessionHandler;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.UnsentRequest;
import org.apache.kafka.clients.consumer.internals.events.CreateFetchRequestsEvent;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.requests.FetchMetadata;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * {@code FetchRequestManager} is responsible for generating {@link FetchRequest} that represent the
 * {@link SubscriptionState#fetchablePartitions(Predicate)} based on the user's topic subscription/partition
 * assignment.
 */
public class FetchRequestManager extends AbstractFetch implements RequestManager {

    private final NetworkClientDelegate networkClientDelegate;
    private CompletableFuture<Void> pendingFetchRequestFuture;

    FetchRequestManager(final LogContext logContext,
                        final Time time,
                        final ConsumerMetadata metadata,
                        final SubscriptionState subscriptions,
                        final FetchConfig fetchConfig,
                        final FetchBuffer fetchBuffer,
                        final FetchMetricsManager metricsManager,
                        final NetworkClientDelegate networkClientDelegate,
                        final ApiVersions apiVersions) {
        super(logContext, metadata, subscriptions, fetchConfig, fetchBuffer, metricsManager, time, apiVersions);
        this.networkClientDelegate = networkClientDelegate;
    }

    @Override
    protected boolean isUnavailable(Node node) {
        return networkClientDelegate.isUnavailable(node);
    }

    @Override
    protected void maybeThrowAuthFailure(Node node) {
        networkClientDelegate.maybeThrowAuthFailure(node);
    }

    /**
     * Signals the {@link Consumer} wants requests be created for the broker nodes to fetch the next
     * batch of records.
     *
     * @see CreateFetchRequestsEvent
     * @return Future on which the caller can wait to ensure that the requests have been created
     */
    public CompletableFuture<Void> createFetchRequests() {
        CompletableFuture<Void> future = new CompletableFuture<>();

        if (pendingFetchRequestFuture != null) {
            // In this case, we have an outstanding fetch request, so chain the newly created future to be
            // completed when the "pending" future is completed.
            pendingFetchRequestFuture.whenComplete((value, exception) -> {
                if (exception != null) {
                    future.completeExceptionally(exception);
                } else {
                    future.complete(value);
                }
            });
        } else {
            pendingFetchRequestFuture = future;
        }

        return future;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PollResult poll(long currentTimeMs) {
        return pollInternal(
            this::prepareFetchRequests,
            this::handleFetchSuccess,
            this::handleFetchFailure
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PollResult pollOnClose(long currentTimeMs) {
        // There needs to be a pending fetch request for pollInternal to create the requests.
        createFetchRequests();

        // TODO: move the logic to poll to handle signal close
        return pollInternal(
                this::prepareCloseFetchSessionRequests,
                this::handleCloseFetchSessionSuccess,
                this::handleCloseFetchSessionFailure
        );
    }

    /**
     * Creates the {@link PollResult poll result} that contains a list of zero or more
     * {@link FetchRequest.Builder fetch requests}.
     *
     * @param fetchRequestPreparer {@link FetchRequestPreparer} to generate a {@link Map} of {@link Node nodes}
     *                             to their {@link FetchSessionHandler.FetchRequestData}
     * @param successHandler       {@link ResponseHandler Handler for successful responses}
     * @param errorHandler         {@link ResponseHandler Handler for failure responses}
     * @return {@link PollResult}
     */
    private PollResult pollInternal(FetchRequestPreparer fetchRequestPreparer,
                                    ResponseHandler<ClientResponse> successHandler,
                                    ResponseHandler<Throwable> errorHandler) {
        if (pendingFetchRequestFuture == null) {
            // If no explicit request for creating fetch requests was issued, just short-circuit.
            return PollResult.EMPTY;
        }

        try {
            Map<Node, FetchSessionHandler.FetchRequestData> fetchRequests = fetchRequestPreparer.prepare();

            List<UnsentRequest> requests = fetchRequests.entrySet().stream().map(entry -> {
                final Node fetchTarget = entry.getKey();
                final FetchSessionHandler.FetchRequestData data = entry.getValue();
                final FetchRequest.Builder request = createFetchRequest(fetchTarget, data);
                final BiConsumer<ClientResponse, Throwable> responseHandler = (clientResponse, error) -> {
                    if (error != null)
                        errorHandler.handle(fetchTarget, data, error);
                    else
                        successHandler.handle(fetchTarget, data, clientResponse);
                };

                return new UnsentRequest(request, Optional.of(fetchTarget)).whenComplete(responseHandler);
            }).collect(Collectors.toList());

            pendingFetchRequestFuture.complete(null);
            return new PollResult(requests);
        } catch (Throwable t) {
            // A "dummy" poll result is returned here rather than rethrowing the error because any error
            // that is thrown from any RequestManager.poll() method interrupts the polling of the other
            // request managers.
            pendingFetchRequestFuture.completeExceptionally(t);
            return PollResult.EMPTY;
        } finally {
            pendingFetchRequestFuture = null;
        }
    }

    /**
     * Create fetch requests for all nodes for which we have assigned partitions that have no existing requests
     * in flight.
     */
    @Override
    protected Map<Node, FetchSessionHandler.FetchRequestData> prepareFetchRequests() {
        // Update metrics in case there was an assignment change
        metricsManager.maybeUpdateAssignment(subscriptions);

        Map<Node, FetchSessionHandler.Builder> fetchable = new HashMap<>();
        long currentTimeMs = time.milliseconds();
        Map<String, Uuid> topicIds = metadata.topicIds();

        // This is the set of partitions that have buffered data
        Set<TopicPartition> buffered = Collections.unmodifiableSet(fetchBuffer.bufferedPartitions());

        // This is the set of partitions that does not have buffered data
        Set<TopicPartition> unbuffered = Set.copyOf(subscriptions.fetchablePartitions(tp -> !buffered.contains(tp)));

        // The first loop is the same logic as in the ClassicKafkaConsumer.
        addFetchables(unbuffered, currentTimeMs, fetchable, topicIds);

        // In the second loop, the AsyncKafkaConsumer does something a little different from the
        // ClassicKafkaConsumer...
        //
        // Make a second pass to loop over all the partitions *with* buffered data. These partitions are included
        // in the fetch requests so that they aren't inadvertently put into the "remove" set. Partitions in the
        // "remove" set are removed from the broker's mirrored cache of fetchable partitions. In some cases, that
        // could then lead to the eviction of the broker's fetch session.
        //
        // In an effort to avoid buffering too much data locally, partitions that already have buffered data only
        // request a nominal fetch size of 1 byte.
        for (TopicPartition partition : buffered) {
            if (!subscriptions.isAssigned(partition)) {
                // It's possible that a partition with buffered data from a previous request is now no longer
                // assigned to the consumer, in which case just skip this partition.
                continue;
            }

            SubscriptionState.FetchPosition position = subscriptions.position(partition);
            Optional<Node> leaderOpt = position.currentLeader.leader;

            if (leaderOpt.isEmpty())
                continue;

            // Use the preferred read replica if set, otherwise the partition's leader
            Node node = selectReadReplica(partition, leaderOpt.get(), currentTimeMs);
            FetchSessionHandler fsh = sessionHandler(node.id());

            if (!isUnavailable(node) &&
                fetchable.containsKey(node) &&
                fsh != null &&
                fsh.sessionId() != FetchMetadata.INVALID_SESSION_ID) {
                addFetchable(fetchable, topicIds, node, partition, position, 1);
            }
        }

        return fetchable.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().build()));
    }

    /**
     * Simple functional interface to all passing in a method reference for improved readability.
     */
    @FunctionalInterface
    protected interface FetchRequestPreparer {

        Map<Node, FetchSessionHandler.FetchRequestData> prepare();
    }
}
