package com.ztore.flink.formats.avro;

import org.apache.avro.Schema;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;

public interface SchemaPublisher{
    void writeSchema(Schema schema, ByteArrayOutputStream out) throws RuntimeException;

    /**
     * Provider for {@link SchemaPublisher}. It allows creating multiple instances of client in
     * parallel operators without serializing it.
     */
    interface SchemaPublisherProvider extends Serializable {

        /**
         * Creates a new instance of {@link SchemaPublisher}. Each time it should create a new
         * instance, as it will be called on multiple nodes.
         *
         * @return new instance {@link SchemaPublisher}
         */
        SchemaPublisher get();
    }
}
