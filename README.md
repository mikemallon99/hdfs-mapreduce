# mp2-cs425

Solution to CS425 MP2 by Robbie Krokos (rkroko2) and Mike Mallon (mmallon3)

# Requirements
python \>= 3.6.8

# how to run
To start network (i.e. the introducer), run the command
```
python3 main.py -start
```

Then, to start a normal node (i.e. not the introducer), run the command
```
python3 main.py join -host <introducer address> -port <port>
```

Once the program has been started, a command line interface will start. Here you can enter commands to begin/interact with the hdfs

# Text Interface Commands:
- `join` Join the group based on the introducer info passed in
- `list` List all member IDs in the group
- `fail` Simulate a node failure, same as sending `SIGTERM` to the node
- `id` List the current nodes ID in the group
- `leave` Leave the group
- `start_hdfs` Starts the distributed file system with the nodes in the current membership list 
    - **Must be in network for this to work**
 ######
*In order to run these commands, you must first run `start_hdfs` on some machine in the group:*
- `master` List the current master ID in the hdfs


