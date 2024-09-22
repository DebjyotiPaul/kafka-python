import socket
import struct
import sys
def create_message(correlation_id: int) -> bytes:
    correlation_id = correlation_id.to_bytes(4, byteorder="big", signed=True)
    message_len = len(correlation_id).to_bytes(4, byteorder="big", signed=False)
    return message_len + correlation_id
def create_message(correlation_id: int, error_code: int | None = None) -> bytes:
    message = correlation_id.to_bytes(4, byteorder="big", signed=True)
    if error_code is not None:
        message += error_code.to_bytes(2, byteorder="big", signed=True)
    message_len = len(message).to_bytes(4, byteorder="big", signed=False)
    return message_len + message
def parse_request(request: bytes) -> dict[str, int | str]:
    buff_size = struct.calcsize(">ihhi")
    length, api_key, api_version, correlation_id = struct.unpack(
        ">ihhi", request[0:buff_size]
    )
    return {
        "length": length,
        "api_key": api_key,
        "api_version": api_version,
        "correlation_id": correlation_id,
    }
def main() -> None:
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    client, _ = server.accept()
    request = client.recv(1024)
    request_data = parse_request(request)
    message = create_message(request_data["correlation_id"])
    if 0 <= request_data["api_version"] <= 4:
        message = create_message(request_data["correlation_id"])
    else:
        message = create_message(
            request_data["correlation_id"], 35
        )  # UNSUPPORTED_VERSION
    client.sendall(message)
    client.close()
    server.close()


if __name__ == "__main__":
    main()
