from membership_list import MembershipList
from membership_list import Status
import time

m = MembershipList()

m.add_node(id="1", host="localhost", port=3256)
print(m)
print("\n")

m.update_state(id="1", seqnum=1231234, timestamp=time.time(), status=Status.FAILED)
m.add_node(id="2", host="localhost", port=5677)
print(m)

print("\n")

for id, state in m:
    print(id, state)
