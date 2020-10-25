# mp2-cs425

Solution to CS425 MP2 by Robbie Krokos (rkroko2) and Mike Mallon (mmallon3)

#### Requirements:
python \>= 3.6.8

# How to run
To start network (i.e. the introducer), run the command
```
python3 main.py --start
```

Then, to start a normal node (i.e. not the introducer), run the command
```
python3 main.py join --host <introducer address> --port <port>
```

Once the program has been started, a command line interface will start. Here you can enter commands to begin/interact with the hdfs

# Text Interface Commands:
- `join` Join the group based on the introducer info passed in
- `list` List all member IDs in the group
- `fail` Simulate a node failure, same as sending `SIGTERM` to the node
- `id` List the current nodes ID in the group
- `leave` Leave the group
- `start_sdfs` Starts the distributed file system with the nodes in the current membership list 
    - **Must be in network for this to work**
    - Can run from any node, the node that runs the command will be chosen as the leader
 ######
*In order to run these commands, you must first run `start_sdfs` on some machine in the group:*
- `put [localfilename] [sdfsfilename]` Stores the localfile in the SDFS
- `get [sdfsfilename] [localfilename]` Retrieves the SDFS file and stores locally as local file
- `delete [sdfsfilename]` Deletes the file from SDFS
- `store` Lists the set of files stored (i.e. replicated) in SDFS at the process that ran this command
- `ls [sdfsfilename]` List all VM addresses where the file is replicated at


