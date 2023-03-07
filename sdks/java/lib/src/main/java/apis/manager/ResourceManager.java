package apis.manager;

import models.Stream;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface ResourceManager {
    CompletableFuture<List<Stream>> createStreams(List<Stream> streams);
}
