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

## Running MapReduce:
- First, you must put all input files you will need into the sdfs
    - These input files must all start with a common prefix
- Then you must put each maple and/or juice executable you want to run
<br>
<br>
- Each maple executable must have 2 functions:
    - def map_format(list)
        - This will take in a list of input lines as an argument
        - This function should take the raw string of lines and return a list of key, value pairs, one for each line
    - def maple(kv_pairs)
        - This will take in a list of 25 key, value pairs as input
        - This should return a list of emitted key, value pairs
- Each juice executable needs only one function:
    - def juice(key, values)
        - The input to this function is a key along with all associated values
        - This should return a list of emitted key and/or values
- To run maple:
    - maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_prefix>
    - `maple_exe` the maple executable to run
    - `num_maples` the number of workers to process the maple tasks
    - `sdfs_intermediate_filename_prefix` the prefix to all output files (format=sdfs_intermediate_filename_prefix_KEY)
    - `sdfs_src_prefix` the prefix of all input files to run maple on
- To run juice:
    - juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> delete_input={0,1}
    - `juice_exe` the juice executable to run
    - `num_juices` the number of workers to process the juice tasks
    - `sdfs_intermediate_filename_prefix` the prefix to all output files from the map phase
    - `sdfs_dest_filename` the filename to store the output to juice
    - `delete_input` whether or not to delete the input files used for the juice tasks

