import socket  # noqa: F401
import struct
import threading


SUPPORTED_VERSIONS = {0, 1, 2, 3, 4}
ERROR_CODE_UNSUPPORTED_VERSION = 35  # Error code for unsupported version


def handle_client(clientsocket, addr):
    while True:
        data = clientsocket.recv(1024)
        print(data)
        mlen = int.from_bytes(data[:4], byteorder="big") # 4 bytes
        request_api_key = int.from_bytes(data[4:6], byteorder="big") # 2 bytes
        request_api_version = int.from_bytes(data[6:8], byteorder="big") # 2 bytes
        correlation_id = int.from_bytes(data[8:12], byteorder="big") # 4 bytes
    
        print(f"message_length: {mlen} \n request_api_key: {request_api_key} \n" +
                f"request_api_version: {request_api_version}\n" +
                f"correlation_id: {correlation_id}")
        if data:
            correlation_id = correlation_id.to_bytes(4, byteorder="big")
            print(f"correlation_id: {correlation_id}")

            response = correlation_id

            if request_api_version not in SUPPORTED_VERSIONS:
                response += ERROR_CODE_UNSUPPORTED_VERSION.to_bytes(2, byteorder="big")
            else: 
                response += int(0).to_bytes(2, byteorder="big")
            
            print(f"response: {response}") 
            response += int(3).to_bytes(1, byteorder="big") # num api keys
            response += request_api_key.to_bytes(2, byteorder="big")
            response += request_api_version.to_bytes(2, byteorder="big")
            response += request_api_version.to_bytes(2, byteorder="big")
            response += int(0).to_bytes(1, byteorder="big") # tag buffer

            response += int(1).to_bytes(2, byteorder="big")
            response += int(16).to_bytes(2, byteorder="big")
            response += int(16).to_bytes(2, byteorder="big")
            response += int(0).to_bytes(1, byteorder="big") # tag buffer
            
            response += int(0).to_bytes(4, byteorder="big") # throttle
            response += int(0).to_bytes(1, byteorder="big") # tag buffer

            print(f"response: {response}") 
            message_length = len(response).to_bytes(4, byteorder="big")
            response = message_length + response
            print(f"response sent: {response}")
            clientsocket.sendall(response)


def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server = socket.create_server(("localhost", 9092), reuse_port=True)

    while True:
        (clientsocket, address)  = server.accept() # wait for client
        if clientsocket:
            print(f"connect by {address}")
            client_thread = threading.Thread(target=handle_client, args=(clientsocket, address))
            client_thread.start()  # Start the thread

if __name__ == "__main__":
    main()
