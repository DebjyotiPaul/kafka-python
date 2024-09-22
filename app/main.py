import socket  # noqa: F401
import threading

def create_message(req):
    # Correlation ID
    message = req[3].to_bytes(4, byteorder="big")
    message += (0 if req[2] in range(5) else 35).to_bytes(
        2, byteorder="big", signed=True
    )
    # Length of array (N+1) as a single byte, followed by list of:
    message += (3).to_bytes(1, byteorder="big", signed=False)
    # API Keys (APIVersions)
    #   API Key    : 18
    #   Min Version:  0
    #   Max Version:  4
    message += req[1].to_bytes(2, byteorder="big", signed=True)
    message += (0).to_bytes(2, byteorder="big", signed=True)
    message += (4).to_bytes(2, byteorder="big", signed=True)
    # TAG_BUFFER
    message += (0).to_bytes(1, byteorder="big", signed=True)
    # API Keys (Fetch)
    #   API Key    :  1
    #   Min Version:  0
    #   Max Version: 16
    message += int(1).to_bytes(2, byteorder="big", signed=True)
    message += (0).to_bytes(2, byteorder="big", signed=True)
    message += (16).to_bytes(2, byteorder="big", signed=True)
    # TAG_BUFFER
    message += (0).to_bytes(1, byteorder="big", signed=True)
    # throttle time
    message += (0).to_bytes(4, byteorder="big", signed=True)
    # TAG_BUFFER
    message += (0).to_bytes(1, byteorder="big", signed=True)
    return len(message).to_bytes(4, byteorder="big", signed=False) + message

def create_message_fetch(req):
    # Correlation ID
    message = req[3].to_bytes(4, byteorder="big")
    # throttle time
    message += (0).to_bytes(4, byteorder="big", signed=True)
    # TAG_BUFFER
    message += (0).to_bytes(1, byteorder="big", signed=True)
    # Error Code
    message += (0).to_bytes(2, byteorder="big", signed=True)
    # Session ID (can be anything)
    message += (0).to_bytes(4, byteorder="big", signed=True)
    # Length of array (N+1) as a single byte, followed by list of:
    message += (1).to_bytes(1, byteorder="big", signed=False)
    # TAG_BUFFER
    message += (0).to_bytes(1, byteorder="big", signed=True)
    return len(message).to_bytes(4, byteorder="big", signed=False) + message

def parse_request(client: socket.socket):
    req = ()
    req_client_id = None
    req_len_bytes = client.recv(4)
    req_len = int.from_bytes(req_len_bytes)
    print("Req Length   :", req_len)
    req_api_key_bytes = client.recv(2)
    req_api_key = int.from_bytes(req_api_key_bytes)
    print("API Key      :", req_api_key)
    req_api_version_bytes = client.recv(2)
    req_api_version = int.from_bytes(req_api_version_bytes)
    print("API Version  :", req_api_version)
    req_corr_id_bytes = client.recv(4)
    req_corr_id = int.from_bytes(req_corr_id_bytes)
    print("CorrelationID:", req_corr_id)
    if req_api_key == 1:  # Fetch
        pass
        # parse "Fetch" Request here
    elif req_api_key == 18:  # ApiVersions
        req_client_id_bytes = client.recv(req_len)
        req_client_id = bytes.decode(req_client_id_bytes, "utf-8")
        # print("ClientID     :", req_client_id)
    req = (req_len, req_api_key, req_api_version, req_corr_id, req_client_id)
    return req

def handle_client(client: socket.socket):
    while True:
        req = parse_request(client)
        print("req", req)
        if req[1] == 1:  # Fetch
            client.sendall(create_message_fetch(req))
        elif req[1] == 18:  # ApiVerisons
            client.sendall(create_message(req))
    # client.close()

def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")
    # Uncomment this to pass the first stage
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    while True:
        client, addr = server.accept()
        t = threading.Thread(target=handle_client, args=(client,))
        t.start()
        
if __name__ == "__main__":
    main()
