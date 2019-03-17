import socket
import sys

from server import BUFFER_SIZE, SERVER_ADDRESS


def query_server(query_message):
    # Create a UDS socket
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

    # Connect the socket to the port where the server is listening
    try:
        sock.connect(SERVER_ADDRESS)
    except socket.error as msg:
        print(msg)
        sys.exit(1)

    try:
        sock.sendall(query_message)

        amount_received = 0
        amount_expected = len(query_message)

        data = b''
        while True:
            part = sock.recv(BUFFER_SIZE)
            data += part
            if len(part) < BUFFER_SIZE:
                break
        return data.decode(encoding='utf-8')

    finally:
        sock.close()


def main():
    query_text = sys.stdin.read()
    sys.stdout.write(query_text)
    sys.stdout.write('\n')
    results = query_server(query_text.encode(encoding='utf-8'))
    sys.stdout.write(results)
    sys.stdout.close()
    sys.stderr.close()


if __name__ == '__main__':
    main()
