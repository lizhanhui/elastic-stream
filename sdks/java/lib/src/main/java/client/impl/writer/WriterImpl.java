package client.impl.writer;

import apis.writer.Writer;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import models.RecordBatch;
import models.RecordMetadata;

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
