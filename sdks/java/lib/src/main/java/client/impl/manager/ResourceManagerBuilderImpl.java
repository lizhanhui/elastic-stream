package client.impl.manager;

import apis.ClientConfiguration;
import apis.manager.ResourceManager;
import apis.manager.ResourceManagerBuilder;
import client.netty.NettyClient;

public class ResourceManagerBuilderImpl implements ResourceManagerBuilder {
    private ClientConfiguration clientConfiguration;

    public ResourceManagerBuilderImpl setClientConfiguration(ClientConfiguration clientConfiguration) {
        this.clientConfiguration = clientConfiguration;
        return this;
    }

    @Override
    public ResourceManager build() throws Exception {
        NettyClient nettyClient = new NettyClient(clientConfiguration);
        return new ResourceManagerImpl(nettyClient);
    }
}
