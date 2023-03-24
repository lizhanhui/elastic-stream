package sdk.elastic.storage.apis.reader;

import sdk.elastic.storage.apis.exception.ClientException;

public interface ReaderBuilder {
    /**
     * Set the stream id to read from.
     *
     * @param streamId the stream id to read from
     */
    void stream(long streamId);

    /**
     * Set the offset to start reading from.
     *
     * @param offset the offset to start reading from
     */
    void startOffset(long offset);

    /**
     * Finalize the builder and create the reader.
     *
     * @return the reader instance
     * @throws ClientException if the reader cannot be created
     */
    Reader build() throws ClientException;
}
