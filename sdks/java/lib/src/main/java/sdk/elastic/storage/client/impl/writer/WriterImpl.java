package sdk.elastic.storage.client.impl.writer;

import sdk.elastic.storage.apis.writer.Writer;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import sdk.elastic.storage.models.RecordBatch;
import sdk.elastic.storage.models.RecordMetadata;

public class WriterImpl implements Writer {
    @Override
    public CompletableFuture<List<RecordMetadata>> append(RecordBatch recordBatch) {
        return null;
    }

    @Override
    public void close() throws IOException {

    }

    private void append0(RecordBatch recordBatch) {

    }
}
