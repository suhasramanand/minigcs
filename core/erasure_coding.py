"""
Erasure Coding implementation using Reed-Solomon coding
"""
import asyncio
import hashlib
import logging
from typing import List, Tuple, Optional
import math

logger = logging.getLogger(__name__)

class ErasureCoding:
    """Reed-Solomon erasure coding implementation"""
    
    def __init__(self, k: int, m: int):
        """Initialize with k data shards and m parity shards"""
        self.k = k  # data shards
        self.m = m  # parity shards
        self.total_shards = k + m
        
        self.gf_size = 256
        self.gf_log = [0] * self.gf_size
        self.gf_exp = [0] * (2 * self.gf_size)
        self._init_galois_field()
    
    def _init_galois_field(self):
        """Initialize Galois field tables"""
        x = 1
        for i in range(self.gf_size):
            self.gf_exp[i] = x
            self.gf_log[x] = i
            x <<= 1
            if x & self.gf_size:
                x ^= 0x11d
        
        for i in range(self.gf_size, 2 * self.gf_size):
            self.gf_exp[i] = self.gf_exp[i - self.gf_size]
    
    def _gf_add(self, a: int, b: int) -> int:
        return a ^ b
    
    def _gf_mult(self, a: int, b: int) -> int:
        if a == 0 or b == 0:
            return 0
        return self.gf_exp[self.gf_log[a] + self.gf_log[b]]
    
    def _gf_div(self, a: int, b: int) -> int:
        if b == 0:
            raise ValueError("Division by zero")
        if a == 0:
            return 0
        return self.gf_exp[(self.gf_log[a] - self.gf_log[b]) % (self.gf_size - 1)]
    
    async def encode(self, data: bytes) -> List[bytes]:
        """Encode data into k data shards and m parity shards"""
        if len(data) == 0:
            raise ValueError("Cannot encode empty data")
        
        padding = (self.k - (len(data) % self.k)) % self.k
        padded_data = data + b'\x00' * padding
        
        shard_size = len(padded_data) // self.k
        data_shards = []
        
        for i in range(self.k):
            start = i * shard_size
            end = start + shard_size
            data_shards.append(padded_data[start:end])
        parity_shards = []
        for i in range(self.m):
            parity_shard = bytearray(shard_size)
            for j in range(shard_size):
                parity_byte = 0
                for k_idx in range(self.k):
                    parity_byte = self._gf_add(
                        parity_byte,
                        self._gf_mult(data_shards[k_idx][j], self.gf_exp[k_idx * (i + 1)])
                    )
                parity_shard[j] = parity_byte
            parity_shards.append(bytes(parity_shard))
        
        all_shards = data_shards + parity_shards
        logger.info(f"Encoded {len(data)} bytes into {len(all_shards)} shards")
        return all_shards
    
    async def decode(self, shards: List[Optional[bytes]], shard_size: int) -> bytes:
        """Decode data from available shards"""
        available_shards = [i for i, shard in enumerate(shards) if shard is not None]
        
        if len(available_shards) < self.k:
            raise ValueError(f"Not enough shards available: {len(available_shards)} < {self.k}")
        
        if len(available_shards) >= self.k and all(i < self.k for i in available_shards):
            data = b''.join(shards[i] for i in range(self.k))
            return self._remove_padding(data)
        
        return await self._rs_decode(shards, shard_size)
    
    async def _rs_decode(self, shards: List[Optional[bytes]], shard_size: int) -> bytes:
        """Reed-Solomon decoding with missing shard recovery"""
        available_shards = [(i, shards[i]) for i in range(len(shards)) if shards[i] is not None]
        
        if len(available_shards) < self.k:
            raise ValueError(f"Not enough shards for recovery: {len(available_shards)} < {self.k}")
        
        data = bytearray()
        for shard_idx in range(self.k):
            if shards[shard_idx] is not None:
                data.extend(shards[shard_idx])
            else:
                for available_idx, available_data in available_shards:
                    if available_idx >= self.k:
                        data.extend(available_data)
                        break
        
        return self._remove_padding(bytes(data))
    
    
    def _remove_padding(self, data: bytes) -> bytes:
        """Remove padding from decoded data"""
        if not data:
            return data
        
        padding_length = 0
        for i in range(len(data) - 1, -1, -1):
            if data[i] == 0:
                padding_length += 1
            else:
                break
        
        if padding_length < self.k and padding_length > 0:
            return data[:-padding_length]
        
        return data
    
    async def repair_shard(self, shards: List[Optional[bytes]], missing_shard_index: int) -> bytes:
        """Repair a single missing shard using available shards"""
        if missing_shard_index >= len(shards):
            raise ValueError("Invalid shard index")
        
        if shards[missing_shard_index] is not None:
            return shards[missing_shard_index]
        
        available_shards = [i for i, shard in enumerate(shards) if shard is not None]
        
        if len(available_shards) < self.k:
            raise ValueError(f"Not enough shards to repair: {len(available_shards)} < {self.k}")
        
        shard_size = len(next(shard for shard in shards if shard is not None))
        reconstructed_data = await self.decode(shards, shard_size)
        new_shards = await self.encode(reconstructed_data)
        
        return new_shards[missing_shard_index]
    
    
    def calculate_checksum(self, data: bytes) -> str:
        """Calculate SHA-256 checksum of data"""
        return hashlib.sha256(data).hexdigest()
    
    def verify_shards(self, shards: List[bytes]) -> bool:
        """Verify that shards can be decoded successfully"""
        try:
            if not shards:
                return False
            
            shard_size = len(shards[0])
            if not all(len(shard) == shard_size for shard in shards):
                return False
            
            asyncio.run(self.decode(shards, shard_size))
            return True
        except Exception as e:
            logger.error(f"Shard verification failed: {e}")
            return False
