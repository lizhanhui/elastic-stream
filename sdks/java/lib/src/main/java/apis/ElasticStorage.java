package apis;

import apis.manager.ResourceManagerBuilder;
import apis.reader.ReaderBuilder;
import apis.writer.WriterBuilder;

public interface ElasticStorage {
    /**
     * Get the reader builder by current provider
     *
     * @return the builder to create a reader instance
     */
    ReaderBuilder newReader();

    /**
     * Get the writer builder by current provider
     *
     * @return the builder to create a writer instance
     */
    WriterBuilder newWriter();

    /**
     * Get the resource manager builder by current provider
     *
     * @return the builder to create a resource manager instance
     */
    ResourceManagerBuilder newResourceManager();
}
