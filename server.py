import socket
import sys
import os


from snowflake_utils import (
    get_results_from_query,
    get_sf_connection,
    get_dict_cursor_from_connection,
)
from row_printer import guess_row_collection

SERVER_ADDRESS = '/tmp/snowflake-socket'
BUFFER_SIZE = 4096

# Make sure the socket does not already exist
def main():
    try:
        os.unlink(SERVER_ADDRESS)
    except OSError:
        if os.path.exists(SERVER_ADDRESS):
            raise

    # Create a UDS socket
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

    # Bind the socket to the address
    print('starting up on {}'.format(SERVER_ADDRESS))
    sock.bind(SERVER_ADDRESS)

    # Listen for incoming connections
    sock.listen(1)

    snowflake_conn = get_sf_connection(database='RTR_QA')

    while True:
        # Wait for a connection
        print('waiting for a connection')
        connection, client_address = sock.accept()
        try:
            print('connection from', client_address)

            # Receive the data in small chunks and retransmit it
            while True:
                data = b''
                while True:
                    part = connection.recv(BUFFER_SIZE)
                    data += part
                    if len(part) < BUFFER_SIZE:
                        break

                print('received {!r}'.format(data))
                if data:
                    print('running query in Snowflake')
                    snowflake_cursor = get_dict_cursor_from_connection(snowflake_conn)
                    results = list(
                        get_results_from_query(
                            data.decode(encoding='utf-8'), snowflake_cursor
                        )
                    )
                    snowflake_cursor.close()
                    row_collection = guess_row_collection(results)
                    for result in results:
                        row_collection.append(result)
                    msg = str(row_collection).encode(encoding='utf-8')
                    print(msg)
                    connection.sendall(msg)
                else:
                    print('no data from', client_address)
                    break

        finally:
            # Clean up the connection
            connection.close()
            snowflake_conn.close()


if __name__ == '__main__':
    main()
