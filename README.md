# Raft in OCaml

To start node and start a cluster:

    dune exec -- bin/main.exe node -host 127.0.0.1  -port 8000

To start a node and join a cluster:

    dune exec -- bin/main.exe node -host 127.0.0.1 -port 8000 -boostrap-server 127.0.0.1:8000
