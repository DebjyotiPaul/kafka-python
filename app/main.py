import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    ##
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    #server.accept() # wait for client
    client_socket, _ = server.accept()  # wait for client
    client_socket.recv(1024)
    msg_len = 0
    correlation_id = 7
    client_socket.sendall(msg_len.to_bytes(4, byteorder="big", signed=True))
    client_socket.sendall(correlation_id.to_bytes(4, byteorder="big", signed=True))
    client_socket.close()
    server.close()


if __name__ == "__main__":
    main()
