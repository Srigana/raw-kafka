import os
import struct

class Partition:
    def __init__(self, id, leader, base_dir):
        self.id = id
        self.leader = leader
        self.followers = []
        self.base_dir = base_dir
        self.next_offset = 0
        self.segments = []
    
    def initialize(self):
        os.makedirs(self.base_dir, exist_ok=True)
        existing = [f for f in os.listdir(self.base_dir) if f.endswith(".log")]
        if existing:
            pass
        else:
            self._create_new_segment(0)
    
    def _create_new_segment(self, base_offset):
        log_path = os.path.join(self.base_dir, f"{base_offset:020d}.log")
        index_path = os.path.join(self.base_dir, f"{base_offset:020d}.index")
        open(log_path, "wb").close()
        open(index_path, "wb").close()
        self.segments.append({"base_offset": base_offset, "log": log_path, "index": index_path})

    def append(self, message):
        active_seg = self.segments[-1]
        with open(active_seg["log"], "ab") as f:
            f.write(struct.pack(">i", len(message)))
            f.write(message)
        self.next_offset += 1
        return self.next_offset - 1

    def read_messages(self, offset, max_bytes):
        active_seg = self.segments[-1]
        messages = []
        bytes_read = 0
        with open(active_seg["log"], "rb") as f:
            while bytes_read < max_bytes:
                length_bytes = f.read(4)
                if not length_bytes:
                    break 
                (length,) = struct.unpack(">i", length_bytes)
                message = f.read(length)
                messages.append(message)
                bytes_read += 4 + length
        return messages





