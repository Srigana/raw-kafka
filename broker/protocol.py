import struct

PRODUCE      = 0x01
FETCH        = 0x02
METADATA     = 0x03
CREATE_TOPIC = 0x04
PRODUCE_RESPONSE  = 0x11
FETCH_RESPONSE    = 0x12
METADATA_RESPONSE = 0x13
ERROR_RESPONSE    = 0x14

REPLICATE          = 0x21
TOPIC_NOTIFICATION = 0x22

def _encode_string(s):
    encoded = s.encode("utf-8")
    return struct.pack(">H", len(encoded)) + encoded

def _decode_string(data, offset):
    (length,) = struct.unpack_from(">H", data, offset)
    offset += 2
    s = data[offset : offset + length].decode("utf-8")
    offset += length
    return s, offset

def _encode_bytes(b):
    return struct.pack(">i", len(b)) + b

def _decode_bytes(data, offset):
    (length,) = struct.unpack_from(">i", data, offset)
    offset += 4
    s = data[offset : offset + length]
    offset += length
    return s, offset

def encode_produce_request(topic, partition, message):
    type = struct.pack(">B", PRODUCE)
    return type + _encode_string(topic) + struct.pack(">i", partition) + _encode_bytes(message)

def encode_fetch_request(topic, partition, offset, max_bytes):
    type = struct.pack(">B", FETCH)
    return type + _encode_string(topic) + struct.pack(">i", partition) + struct.pack(">q", offset) +  struct.pack(">i", max_bytes)

def encode_metadata_request():
    return struct.pack(">B", METADATA)

def encode_create_topic_request(topic, num_partitions, replication_factor):
    type = struct.pack(">B", CREATE_TOPIC)
    return type + _encode_string(topic) + struct.pack(">i", num_partitions) + struct.pack(">H", replication_factor)

def decode_produce_response(data):
    pos = 0
    (type,) = struct.unpack_from(">B", data, pos)
    
    pos += 1
    if type == ERROR_RESPONSE:
        error_msg, _ = _decode_string(data, pos)
        return None
    
    (written_offset,) = struct.unpack_from(">q", data, pos) 
    pos += 8
    (status,) = struct.unpack_from(">B", data, pos)

    if status == 0:
        return written_offset
    else:
        return None
    
def decode_fetch_response(data):
    pos = 0
    (type,) = struct.unpack_from(">B", data, pos)
    pos += 1
    if type == ERROR_RESPONSE:
        error_msg, _ = _decode_string(data, pos)
        return None
    (msg_count,) = struct.unpack_from(">i", data, pos)
    pos += 4
    messages = []
    for _ in range(msg_count):
        pos += 8  
        msg, pos =  _decode_bytes(data, pos)
        messages.append(msg)
    return messages

def decode_metadata_response(data):
    pos = 0
    (type,) = struct.unpack_from(">B", data, pos)
    pos += 1
    if type == ERROR_RESPONSE:
        error_msg, _ = _decode_string(data, pos)
        return None
    
    (broker_count,) = struct.unpack_from(">i", data, pos)
    pos += 4
    brokers = []
    for _ in range(broker_count):
        (id,) = struct.unpack_from(">i", data, pos)
        pos += 4
        host, pos = _decode_string(data, pos)
        (port,) = struct.unpack_from(">i", data, pos)
        pos += 4
        brokers.append({"id": id, "host": host, "port": port})
    
    (topic_count,) = struct.unpack_from(">i", data, pos)
    pos += 4
    topics = []
    for _ in range(topic_count):
        topic_name, pos = _decode_string(data, pos)
        (partition_count,) = struct.unpack_from(">i", data, pos)
        pos += 4
        partitions = []
        for _ in range(partition_count):
            (partition_id,) = struct.unpack_from(">i", data, pos)
            pos += 4
            (leader_id,) = struct.unpack_from(">i", data, pos)
            pos += 4
            partitions.append({"partition_id": partition_id, "leader_id": leader_id})
        topics.append({"topic": topic_name, "partitions": partitions})

    return ({"brokers": brokers, "topics": topics})

def send_error_response(sock, error_message):
    payload = struct.pack(">B", ERROR_RESPONSE) + _encode_string(error_message)
    sock.sendall(payload)
