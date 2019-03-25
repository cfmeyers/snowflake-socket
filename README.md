# Keep Snowflake Connection Alive
with client that connects to an always-on server via Unix Sockets.

Give vim (or your text editor of choice) a Snowflake REPL-like capability.

# Install
- clone this repo
- cd into the repo
```shell
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python snowflake_proxy_server.py --connection_header connections.my_connection_header
```
where `connections.my_connection_header` is in your `~./snowsql/config` file.

Pipe your query to `client.py`; `client.py` will connect to `server.py` via a Unix socket.  `server.py` (started separately) has a connection to snowflake, kept alive.  If > 20 minutes have passed since the last call to Snowflake, `server.py` creates a new connection.
`server.py` sends the query to Snowflake, gets the results back, pretty-formats the resuls, and sends them back to the client.

By using this client-server architecture (rather than simply calling Snowflake directly) we save the 2-3 seconds of time taken up by establishing a conenction to Snowflake.  

The following vimscript gives you the ability to highlight a query and run it in Snowflake from Vim (similar to Emacs' Org-Mode or Jupyter Notebooks).
```
fun! Fetch_SF_query_results() range
    :'<,'>: !/path/to/snowflake-socket/venv/bin/python /path/to/snowflake-socket/client.py
endfun
vnoremap ,f :call Fetch_SF_query_results()<cr>
```
where `/path/to/snowflake-socket` is the path to this directory.
