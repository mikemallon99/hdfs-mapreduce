# Requirements
python \>= 3.6

# How to run
To start a introducer instance, run
```
python3 main.py --introducer
```

To start a non introducer instance, run
```
python3 main.py --introducer-host <host name> --introducer-port <port number>
```

See all available command line flags and what they are for, run
```
python3 main.py -h
```

Here is an overview:
1. `--host` host name of this server, optional.
2. `--port` which port number to run this service, optional.
3. `--failure-detection-time` in seconds, optional. Defaults to 5 seconds.
4. `--dissemination-time` in seconds, optional. Defaults to 6 seconds.
5. `--protocol` This is the initial procotol that the service will use. Either `GOSSIP` or `ALL2ALL`. Defaults to `ALL2ALL`.
6. `--introducer` Whether this node is a introducer, defaults to `False`.
7. If the current node is not a introducer, `--introducer-host` and `--introducer-port` must be passed in.
8. `--message-failure-rate` A number between 0 and 1. This node will drop messages at this probability before sending. Defaults to 0.

# Interactive commands
After starting the instance, you can use the following commands to interact with the service:
- `switch` Switch between the two protocols on this instance.
- `list` List all members in current membership list. If not in a group, nothing will be listed.
- `id` Print the id of this node in the group. If not in a group, id is `None`.
- `join` Join the group based on the introducer information passed in.
- `leave` Leave the group.
- `fail` Simulate node failure. The same as sending `SIGTERM` to this node.