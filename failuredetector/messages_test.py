from messages import MessageType, create_message, parse_and_validate_message
from membership_list import MembershipList

# Test creating and parsing messages
# JOIN
join_msg = create_message(MessageType.JOIN, id="1")
print(join_msg)
parsed_join_msg = parse_and_validate_message(join_msg)
print(parsed_join_msg)

# JOIN_RES
mem_list = MembershipList()
mem_list.add_node("2", host="localhost", port=6696)
join_res_msg = create_message(MessageType.JOIN_RES, mem_list=mem_list)
print(join_res_msg)
parsed_join_res_msg = parse_and_validate_message(join_res_msg)
print(parsed_join_res_msg)

# HEARTBEAT
seqnum = 19462
heartbeat_msg = create_message(MessageType.HEARTBEAT, id="node03", seqnum=seqnum)
print(heartbeat_msg)
parsed_heartbeat_msg = parse_and_validate_message(heartbeat_msg)
print(parsed_heartbeat_msg)

# invalidate heartbeat with no sequence number
invalid_heartbeat = parse_and_validate_message(b'{"id": "node03", "type": "HEARTBEAT"}')
print(f"Parsed invalid heartbeat: {invalid_heartbeat}")
# invalidate message with no type
invalid_heartbeat = parse_and_validate_message(b'{"id": "node03", "seqnum": "8866086"}')
print(f"Parsed invalid heartbeat: {invalid_heartbeat}")
# invalidate heartbeat with non existant type
invalid_heartbeat = parse_and_validate_message(b'{"id": "node03", "type": "BEAT"}')
print(f"Parsed invalid heartbeat: {invalid_heartbeat}")

try:
    # the following should throw an exception because "id" is not provided
    create_message(MessageType.JOIN)
    print("NOOOOO I FAILED")
except ValueError as e:
    print(e)
