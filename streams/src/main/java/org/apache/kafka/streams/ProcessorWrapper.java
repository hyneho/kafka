package org.apache.kafka.streams;

import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;

/**
 * Wrapper class that can be used to inject custom wrappers around the processors of their application topology.
 * The returned instance MUST wrap the supplied {@code ProcessorSupplier} and the {@code Processor} it supplies.
 * Returning a new or completely different instance can have unexpected and undesirable affects.
 */
public interface ProcessorWrapper {

    <KIn, VIn, KOut, VOut> ProcessorSupplier<KIn, VIn, KOut, VOut> wrapProcessorSupplier(final String processorName,
                                                                                         final ProcessorSupplier<KIn, VIn, KOut, VOut> processorSupplier);

    <KIn, VIn,  VOut> FixedKeyProcessorSupplier<KIn, VIn,  VOut> wrapFixedKeyProcessorSupplier(final String processorName,
                                                                                               final FixedKeyProcessorSupplier<KIn, VIn, VOut> processorSupplier);
}
