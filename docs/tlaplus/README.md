# ElasticStream Replication Protocol TLA+ Specification

## Opening the specifications 

### With the ToolBox

The constants are sufficiently described in the specifications for anyone to create a model using the toolbox.

### Via cmd line

Each spec has an associated cfg file that can be used when running from cmd line.
 
```
wget https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar
wget https://github.com/tlaplus/CommunityModules/releases/download/202102040137/CommunityModules.jar
java -cp tla2tools.jar:CommunityModules.jar tlc2.TLC ElasticStream -tool -modelcheck -deadlock -config ElasticStream.cfg -workers auto
```

### VSCode-tlaplus(Recommend)
https://github.com/tlaplus/vscode-tlaplus/wiki/Getting-Started