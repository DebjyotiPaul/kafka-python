import socket  # noqa: F401
import threading
from enum import Enum
MIN_API_VER = 0
MAX_API_VER = 4
class RespErrCode(Enum):
    NO_ERROR = 0
    UNSUPPORTED_VERSION = 35
def build_resp(correlation_id: int, api_key: int, api_version: int):
    header = correlation_id.to_bytes(4)
    TAG_BUFFER = (0).to_bytes(1)
    err_code = RespErrCode.NO_ERROR.value
    if not (MIN_API_VER <= api_version <= MAX_API_VER):
        err_code = RespErrCode.UNSUPPORTED_VERSION.value
    num_api_keys = 1
    throttle_time_ms = 0
    body = (
        err_code.to_bytes(2)
        + (num_api_keys + 1).to_bytes(1)
        + api_key.to_bytes(2)
        + MIN_API_VER.to_bytes(2)
        + MAX_API_VER.to_bytes(2)
        + TAG_BUFFER
        + throttle_time_ms.to_bytes(4)
        + TAG_BUFFER
    )
    resp = header + body
    resp = len(resp).to_bytes(4) + resp
    return resp
def api_versions_resp(request: bytes):
    _, header = request[:4], request[4:]
    req_api_key = int.from_bytes(header[:2])
    req_api_version = int.from_bytes(header[2:4])
    correlation_id = int.from_bytes(header[4:8])
    return build_resp(correlation_id, req_api_key, req_api_version)

def respond(client: socket.socket):
    while True:
        req = client.recv(1024)
        resp = api_versions_resp(req)
        client.send(resp)
def main():
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    while True:
        client, addr = server.accept()
        t = threading.Thread(target=respond, args=(client,))
        t.start()
if __name__ == "__main__":
    main()

