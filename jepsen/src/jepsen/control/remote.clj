(ns jepsen.control.remote
  "The Remote protocol provides a common interface for running commands and
  uploading files to nodes.")

(defprotocol Remote
  (connect [this conn-spec]
    "Set up the remote to work with a particular node. Returns a Remote which
    is ready to accept actions via `execute!` and `upload!` and `download!`.
    conn-spec is a map of:

     {:host
      :post
      :username
      :password
      :private-key-path
      :strict-host-key-checking}
    ")

  (disconnect! [this]
    "Disconnect a remote that has been connected to a host.")

  (execute! [this action]
    "Execute the specified action in a remote connected a host. Action is a map
    with keys:

      :cmd   A string command to execute.
      :in    A string to provide for the command's stdin.

    Should return the action map with additional keys:

      :exit  The command's exit status.
      :out   The stdout string.
      :err   The stderr string.
    ")

  (upload! [this local-paths remote-path rest]
    "Copy the specified local-path to the remote-path on the connected host.
    The `rest` argument is a sequence of additional arguments to be
    interpreted by the underlying implementation; for example, with a clj-ssh
    remote, these args are the remainder args to `scp-to`.")

  (download! [this remote-paths local-path rest]
    "Copy the specified remote-paths to the local-path on the connected host.
    The `rest` argument is a sequence of additional arguments to be
    interpreted by the underlying implementation; for example, with a clj-ssh
    remote, these args are the remainder args to `scp-from`."))
