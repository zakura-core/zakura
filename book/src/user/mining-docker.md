# Mining with Zakura in Docker

Zakura's [Docker images](https://hub.docker.com/r/valargroup/zakura/tags) can be used
for your mining operations. If you don't have Docker, see the [manual
configuration instructions](mining.md).

Using docker, you can start mining by running:

```bash
docker run -d --name zakura_local \
  -e MINER_ADDRESS="t3dvVE3SQEi7kqNzwrfNePxZ1d4hUyztBA1" \
  -e ZAKURA_RPC__LISTEN_ADDR=0.0.0.0:8232 \
  -p 8233:8233 \
  -p 8232:8232 \
  -v zakurad-cache:/home/zakura/.cache/zakura \
  valargroup/zakura:latest
```

This command starts a container on Mainnet and binds the P2P port (8233) and
the RPC port (8232) on your Docker host. The P2P port lets other Zcash nodes
connect to your node. If you want to start generating blocks, you need to let
Zakura sync first.

Note that you must pass the address for your mining rewards via the
`MINER_ADDRESS` environment variable when you are starting the container, as we
did with the ZF funding stream address above. The address we used starts with
the prefix `t1`, meaning it is a Mainnet P2PKH address. Please remember to set
your own address for the rewards.

Instead of listing the environment variables on the command line, you can use
Docker's `--env-file` flag to specify a file containing the variables. You can
find more info here
<https://docs.docker.com/engine/reference/commandline/run/#env>.

If you don't want to set any environment variables, you can edit the
`docker/default-zakura-config.toml` file, and pass it to Zakura before starting
the container. There's an example in `docker/docker-compose.yml` of how to do
that.

If you want to mine on Testnet, you need to set the `ZAKURA_NETWORK__NETWORK` environment
variable to `Testnet` and use a Testnet address for the rewards. For example,
running

```bash
docker run -d --name zakura_local \
  -e ZAKURA_NETWORK__NETWORK="Testnet" \
  -e MINER_ADDRESS="t27eWDgjFYJGVXmzrXeVjnb5J3uXDM9xH9v" \
  -e ZAKURA_RPC__LISTEN_ADDR=0.0.0.0:18232 \
  -p 18233:18233 \
  -p 18232:18232 \
  -v zakurad-cache:/home/zakura/.cache/zakura \
  valargroup/zakura:latest
```

will start a container on Testnet and bind the P2P port (18233) and the RPC port
(18232) on your Docker host. Notice that we also used a different rewards
address. It starts with the prefix `t2`, indicating that it is a Testnet
address. A Mainnet address would prevent Zakura from starting on Testnet, and
conversely, a Testnet address would prevent Zakura from starting on Mainnet.

To connect to the RPC port, you will need the contents of the [cookie
file](mining.md?highlight=cookie#testing-the-setup)
Zakura uses for authentication. By default, it is stored at
`/home/zakura/.cache/zakura/.cookie`. You can print its contents by running

```bash
docker exec -it zakura_local cat /home/zakura/.cache/zakura/.cookie
```

If you want to avoid authentication, you can turn it off by setting

```toml
[rpc]
enable_cookie_auth = false
```

in Zakura's config file before you start the container.
