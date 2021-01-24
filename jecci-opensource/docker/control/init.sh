#!/bin/sh

: "${SSH_PRIVATE_KEY?SSH_PRIVATE_KEY is empty, please use up.sh}"
: "${SSH_PUBLIC_KEY?SSH_PUBLIC_KEY is empty, please use up.sh}"

if [ ! -f ~/.ssh/known_hosts ]; then
    mkdir -m 700 ~/.ssh
    echo $SSH_PRIVATE_KEY | perl -p -e 's/↩/\n/g' > ~/.ssh/id_rsa
    chmod 600 ~/.ssh/id_rsa
    echo $SSH_PUBLIC_KEY > ~/.ssh/id_rsa.pub
    echo > ~/.ssh/known_hosts
    for f in $(seq 1 5); do
      ssh-keyscan -t rsa n$f >> ~/.ssh/known_hosts
    done
fi

# TODO: assert that SSH_PRIVATE_KEY==~/.ssh/id_rsa

# for elle to work
export DISPLAY=:0
X &

cat <<EOF
Welcome to Jecci on Docker
===========================

Please run \`docker exec -it jecci-control bash\` in another terminal to proceed.
EOF

# hack for keep this container running
tail -f /dev/null
