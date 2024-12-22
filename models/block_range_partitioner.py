from loguru import logger


class BlockRangePartitioner:
    def __init__(self, num_partitions: int = 39):
        self.num_partitions = num_partitions
        self.range_size = 52560  # 144 blocks/day * 365 days

        self.partition_ranges = {
            i: (i * self.range_size, (i + 1) * self.range_size - 1)
            for i in range(num_partitions)
        }
        logger.info("Initialized partitioner",
                    partition_ranges=self.partition_ranges,
                    blocks_per_year=self.range_size)

    def __call__(self, block_height):
        partition = block_height // self.range_size
        if partition >= self.num_partitions:
            partition = self.num_partitions - 1
        return int(partition)

    def get_partition_range(self, partition):
        start = partition * self.range_size
        end = (partition + 1) * self.range_size - 1
        if partition == self.num_partitions - 1:
            end = float('inf')
        return (start, end)

    def align_height_to_partition(self, height: int, round_up: bool = False) -> int:
        """Align height to partition boundary"""
        partition = height // self.range_size
        if round_up:
            return (partition + 1) * self.range_size
        return partition * self.range_size

    def get_partition_for_range(self, start_height: int, end_height: int = None) -> list:
        """Get list of partitions that cover the given height range"""
        start_partition = self.__call__(start_height)
        if end_height is None:
            return list(range(start_partition, self.num_partitions))
        end_partition = self.__call__(end_height)
        return list(range(start_partition, end_partition + 1))
