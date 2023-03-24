package sdk.elastic.storage.client.impl;

import sdk.elastic.storage.apis.ClientConfiguration;
import sdk.elastic.storage.apis.OperationClient;
import sdk.elastic.storage.apis.OperationClientBuilder;
import com.google.common.base.Preconditions;

public class OperationClientBuilderImpl implements OperationClientBuilder {
    private ClientConfiguration clientConfiguration;
    @Override
    public OperationClientBuilder setClientConfiguration(ClientConfiguration clientConfiguration) {
        Preconditions.checkArgument(clientConfiguration != null, "clientConfiguration is null");
        this.clientConfiguration = clientConfiguration;
        return this;
    }

    @Override
    public OperationClient build() throws Exception {
        return new OperationClientImpl(clientConfiguration);
    }
}
