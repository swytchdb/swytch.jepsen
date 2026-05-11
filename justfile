nodes := "n1.swytch.earth,n2.swytch.earth,n3.swytch.earth,n4.swytch.earth,n5.swytch.earth"
ssh_key := "~/.ssh/id_rsa"
time_limit := "60"

# DNS name that resolves to every peer's cluster-port address. Managed
# out-of-band by the operator (e.g. via an A record pointing at every
# node IP). Override with `just join_dns=foo.example.com ...`.
join_dns := "cluster.swytch.earth"

_run workload nemesis *extra_args:
    lein run test --nodes {{nodes}} --ssh-private-key {{ssh_key}} --workload {{workload}} --nemesis-config {{nemesis}} --join-dns {{join_dns}} --time-limit {{time_limit}} {{extra_args}}

# Safe nemesis — redis transport
safe-counter *args: (_run "counter" "safe" args)
safe-set *args: (_run "set" "safe" args)
safe-sorted-set *args: (_run "sorted-set" "safe" args)
safe-elle-causal *args: (_run "elle-causal" "safe" args)

# Safe nemesis — sql transport
safe-sql-append *args: (_run "sql-append" "safe" args)

# No nemesis — redis transport
none-counter *args: (_run "counter" "none" args)
none-set *args: (_run "set" "none" args)
none-sorted-set *args: (_run "sorted-set" "none" args)
none-elle-causal *args: (_run "elle-causal" "none" args)

# No nemesis — sql transport
none-sql-append *args: (_run "sql-append" "none" args)

# Run every workload with a given nemesis
safe-all: safe-counter safe-set safe-sorted-set safe-elle-causal safe-sql-append
none-all: none-counter none-set none-sorted-set none-elle-causal none-sql-append
