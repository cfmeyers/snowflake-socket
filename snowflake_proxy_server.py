import argparse
import socket
import sys
import os
from datetime import datetime, timedelta


from snowflake_utils import (
    get_results_from_query,
    get_sf_connection,
    get_dict_cursor_from_connection,
)
from row_printer import guess_row_collection

SERVER_ADDRESS = '/tmp/snowflake-proxy.socket'
BUFFER_SIZE = 4096
REFRESH_INTERVAL_IN_MINUTES = 20
RESULTS_LIMIT = 500


def parse_args():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        '-c',
        '--connection_header',
        help="Snowflake connection header for .snowsql/config file",
        type=str,
        required=True,
    )
    return parser.parse_args()


def refresh_sf_connection(snowflake_conn, time_of_last_connection, connection_header):
    now = datetime.now()
    minutes_since_last_connection = (now - time_of_last_connection).seconds / 60
    print(f'{minutes_since_last_connection} minutes since last snowflake conn refresh')
    if minutes_since_last_connection > REFRESH_INTERVAL_IN_MINUTES:
        print('refreshing snowflake connection')
        snowflake_conn.close()
        new_connection = get_sf_connection(connection_header)
        print('Snowflake connection refreshed')
        return new_connection, now
    else:
        print('no need to refresh snowflake connection')
        return snowflake_conn, now


def format_msg(row_collection, results_were_truncated):
    if results_were_truncated:
        coda = f'\n*Results truncated, only displaying top {RESULTS_LIMIT}*\n'
    else:
        coda = ''
    msg = str(row_collection)
    msg += coda
    return msg.encode(encoding='utf-8')


def main(connection_header):
    try:
        # Make sure the socket does not already exist
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

    snowflake_conn = get_sf_connection(connection_header)
    time_of_last_connection = datetime.now()

    while True:
        # Wait for a connection
        print('waiting for a connection')
        connection, client_address = sock.accept()
        try:
            print('connection from', client_address)

            # Receive the data in small chunks and retransmit it
            data = b''
            while True:
                part = connection.recv(BUFFER_SIZE)
                data += part
                if len(part) < BUFFER_SIZE:
                    break

            print('received {!r}'.format(data))
            if data:
                print('running query in Snowflake')
                snowflake_conn, time_of_last_connection = refresh_sf_connection(
                    snowflake_conn, time_of_last_connection, connection_header
                )
                snowflake_cursor = get_dict_cursor_from_connection(snowflake_conn)
                query = data.decode(encoding='utf-8')
                try:
                    results_gen = get_results_from_query(query, snowflake_cursor)
                    results = [r for r in results_gen][: RESULTS_LIMIT + 1]
                    if len(results) == RESULTS_LIMIT + 1:
                        results_were_truncated = True
                        results = results[:RESULTS_LIMIT]
                    else:
                        results_were_truncated = False
                    snowflake_cursor.close()
                    row_collection = guess_row_collection(results)
                    for result in results:
                        row_collection.append(result)
                    msg = format_msg(row_collection, results_were_truncated)
                except Exception as e:
                    print(e)
                    msg = str(e).encode(encoding='utf-8')
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
    args = parse_args()
    main(args.connection_header)
