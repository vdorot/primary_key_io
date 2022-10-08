import docker


def get_container_io(container_name):
    client = docker.from_env()
    container = client.containers.get(container_name)

    stats = container.stats(stream=False)

    blkio_stats = stats["blkio_stats"]["io_service_bytes_recursive"]
    if blkio_stats is None:
        return 0, 0
    total_reads = sum(dev["value"] for dev in blkio_stats if dev["op"] == "read")
    total_writes = sum(dev["value"] for dev in blkio_stats if dev["op"] == "write")

    return total_reads, total_writes
