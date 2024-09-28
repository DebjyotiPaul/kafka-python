import socket  # noqa: F401
from app.kafka_types import *
import threading
FETCH = 1
API_VERSION = 18
api_versions = {
    FETCH: (0, 16),
    API_VERSION: (0, 4),
}
def header_request_v2(req: bytes) -> dict:
    # msg_len = get_int32(req[:4])
    data = dict()
    data["api_key"] = get_int16(req[4:6])
    data["api_ver"] = get_int16(req[6:8])
    data["correlation_id"] = req[8:12]
    data["client_id"], req = parse_string(req[12:])
    data["tags"] = req[0:1]  # not implemented here
    data["body"] = req[1:]
    return data
def request_apiversion_v3(req: bytes) -> dict:
    data = dict()
    data["client_software_name"], req = parse_compact_string(req)
    data["client_software_version"], req = parse_compact_string(req)
    data["tags"] = req
    return data
def parse_partitions(req: bytes) -> tuple[list, bytes]:
    partitions_len, req = get_varint(req)
    partitions_len -= 1
    print("partitions_len", partitions_len)
    partitions = list()
    for _ in range(partitions_len):
        data = dict()
        data["partition"] = get_int32(req[:4])
        data["current_leader_epoch"] = get_int32(req[4:8])
        data["fetch_offset"] = get_int64(req[8:16])
        data["last_fetched_epoch"] = get_int32(req[16:20])
        data["log_start_offset"] = get_int64(req[20:28])
        data["partition_max_bytes"] = get_int32(req[28:32])
        data["tag"] = req[32:33]
        partitions.append(data)
        req = req[33:]
    return partitions, req
def parse_forgotten_partitions(req: bytes) -> tuple[list, bytes]:
    partitions_len, req = get_varint(req)
    partitions_len -= 1
    print("fg_partitions_len", partitions_len)
    partitions = list()
    for _ in range(partitions_len):
        partitions.append(get_int32(req[:4]))
        req = req[4:]
    return partitions, req
def request_fetch_v16(req: bytes) -> dict:
    data = dict()
    data["max_wait_ms"] = get_int32(req[0:4])
    data["min_bytes"] = get_int32(req[4:8])
    data["max_bytes"] = get_int32(req[8:12])
    data["isolation_level"] = get_int8(req[12:13])
    data["session_id"] = get_int32(req[13:17])
    data["session_epoch"] = get_int32(req[17:21])
    ## parse topics
    topics_len, req = get_varint(req[21:])
    topics_len -= 1
    print("topics_len", topics_len)
    topics = list()
    for _ in range(topics_len):
        topic = dict()
        topic["topic_id"], req = get_uuid(req)
        topic["partitions"], req = parse_partitions(req)
        topic["tags"] = req[0]
        topics.append(topic)
        req = req[1:]
    data["topics"] = topics
    ## parse forgotten_topics_data
    fg_topics_len, req = get_varint(req)
    fg_topics_len -= 1
    print("fg_topics_len", fg_topics_len)
    fg_topics = list()
    for _ in range(fg_topics_len):
        fg_topic = dict()
        fg_topic["topic_id"], req = get_uuid(req)
        fg_topic["partitions"], req = parse_forgotten_partitions(req)
        fg_topics.append(fg_topic)
    data["forgotten_topics_data"] = fg_topics
    data["rack_id"], req = parse_compact_string(req)
    data["tags"] = req
    return data
def make_response_v0(fields: list[bytes]) -> bytes:
    resp = b"".join(fields)
    resp = to_int32(len(resp)) + resp
    return resp

def make_header_v1(header: dict) -> bytes:
    return header["correlation_id"]
def make_header_v2(header: dict) -> bytes:
    return header["correlation_id"] + to_tagbuffer(None)
def response_apiversion_v3() -> bytes:
    apis = list()
    for key, versions in api_versions.items():
        apis.append(
            b"".join(
                [
                    to_int16(key),
                    to_int16(versions[0]),
                    to_int16(versions[1]),
                    to_tagbuffer(None),
                ]
            )
        )
    return to_array(apis)
def response_fetch_v16(topics: list) -> bytes:
    responses = list()
    for topic in topics:
        response = topic["topic_id"]
        partitions = list()
        for partition in topic["partitions"]:
            partition = to_int32(0)  # partition_index
            partition += to_int16(100)  # error_code
            partition += to_int64(0)  # high_watermark
            partition += to_int64(0)  # last_stable_offset
            partition += to_int64(0)  # log_start_offset
            partition += to_array([])  # aborted_transactions
            partition += to_int32(0)  # preferred_read_replica
            partition += to_varint(0)  # records => COMPACT_RECORDS
            partition += to_tagbuffer(None)
            partitions.append(partition)
        response += to_array(partitions)
        response += to_tagbuffer(None)
        responses.append(response)
    return to_array(responses)
def handle(client: socket.socket):
    req = client.recv(1024)
    while True:
        msg_len = get_int32(req[:4])
        remaining = req[msg_len + 4 :]
        req = req[: msg_len + 4]
        header = header_request_v2(req)
        print(header)
        print(f"Request:", header["api_key"])
        if header["api_key"] == FETCH:
            details = request_fetch_v16(header["body"])
            print(details)

            client.sendall(
                make_response_v0(
                    [make_header_v2(header),
                        to_int32(0),  # throttle
                        to_int16(0),  # error code
                        to_int32(details["session_id"]),
                        response_fetch_v16(details["topics"]),
                        to_tagbuffer(None),  # tagged buf
                    ]
                )
            )
        elif header["api_key"] == API_VERSION:
            details = request_apiversion_v3(header["body"])
            print(details)
            if header["api_ver"] in [0, 1, 2, 3, 4]:
                client.sendall(
                    make_response_v0(
                        [
                            make_header_v1(header),
                            to_int16(0),  # error code
                            response_apiversion_v3(),
                            to_int32(0),  # throttle
                            to_tagbuffer(None),  # tagged buf
                        ]
                    )
                )
            else:
                client.sendall(
                    make_response_v0([header["correlation_id"], to_int16(35)])
                )
        remaining += client.recv(1024)
        if len(remaining) == 0:
            client.close()
            break
        else:
            req = remaining
def main():
    print("Logs from your program will appear here!")
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    while True:
        client, addr = server.accept()  # wait for client
        threading.Thread(target=handle, args=(client,)).start()
    # handle(client)
if __name__ == "__main__":
    main()