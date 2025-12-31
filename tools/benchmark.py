#!/usr/bin/env python3
"""
MyDB Benchmark Script

This script performs load testing against a MyDB server.
"""

import socket
import struct
import time
import random
import string
import argparse
import threading
from dataclasses import dataclass
from typing import Optional
import statistics


@dataclass
class BenchmarkResult:
    operation: str
    total_ops: int
    duration_sec: float
    ops_per_sec: float
    latencies_ms: list
    
    @property
    def avg_latency_ms(self) -> float:
        return statistics.mean(self.latencies_ms) if self.latencies_ms else 0
    
    @property
    def p50_latency_ms(self) -> float:
        if not self.latencies_ms:
            return 0
        sorted_lat = sorted(self.latencies_ms)
        return sorted_lat[len(sorted_lat) // 2]
    
    @property
    def p99_latency_ms(self) -> float:
        if not self.latencies_ms:
            return 0
        sorted_lat = sorted(self.latencies_ms)
        idx = int(len(sorted_lat) * 0.99)
        return sorted_lat[min(idx, len(sorted_lat) - 1)]


class MyDBClient:
    """Simple client for MyDB protocol."""
    
    OP_PUT = 0x01
    OP_DELETE = 0x02
    OP_GET = 0x03
    OP_PING = 0x05
    OP_STATUS = 0x06
    
    def __init__(self, host: str = "127.0.0.1", port: int = 6379):
        self.host = host
        self.port = port
        self.sock: Optional[socket.socket] = None
    
    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
    
    def close(self):
        if self.sock:
            self.sock.close()
            self.sock = None
    
    def _encode_string(self, s: str) -> bytes:
        encoded = s.encode('utf-8')
        return struct.pack('<I', len(encoded)) + encoded
    
    def _send_request(self, opcode: int, payload: bytes = b''):
        header = struct.pack('<BI', opcode, len(payload))
        self.sock.sendall(header + payload)
    
    def _recv_response(self) -> bytes:
        # Simple receive - in production would need proper framing
        return self.sock.recv(4096)
    
    def put(self, key: str, value: str) -> bool:
        payload = self._encode_string(key) + self._encode_string(value)
        self._send_request(self.OP_PUT, payload)
        response = self._recv_response()
        return response and response[0] == 0x00
    
    def get(self, key: str) -> Optional[str]:
        payload = self._encode_string(key)
        self._send_request(self.OP_GET, payload)
        response = self._recv_response()
        if response and response[0] == 0x00:
            # Skip response code, parse string
            length = struct.unpack('<I', response[1:5])[0]
            return response[5:5+length].decode('utf-8')
        return None
    
    def delete(self, key: str) -> bool:
        payload = self._encode_string(key)
        self._send_request(self.OP_DELETE, payload)
        response = self._recv_response()
        return response and response[0] == 0x00
    
    def ping(self) -> bool:
        self._send_request(self.OP_PING)
        response = self._recv_response()
        return response and response[0] == 0x00


def random_string(length: int) -> str:
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def benchmark_puts(client: MyDBClient, num_ops: int, key_size: int, value_size: int) -> BenchmarkResult:
    latencies = []
    
    start = time.time()
    for i in range(num_ops):
        key = f"bench_key_{i:010d}"
        value = random_string(value_size)
        
        op_start = time.time()
        client.put(key, value)
        op_end = time.time()
        
        latencies.append((op_end - op_start) * 1000)
    
    duration = time.time() - start
    
    return BenchmarkResult(
        operation="PUT",
        total_ops=num_ops,
        duration_sec=duration,
        ops_per_sec=num_ops / duration,
        latencies_ms=latencies
    )


def benchmark_gets(client: MyDBClient, num_ops: int) -> BenchmarkResult:
    latencies = []
    
    start = time.time()
    for i in range(num_ops):
        key = f"bench_key_{i:010d}"
        
        op_start = time.time()
        client.get(key)
        op_end = time.time()
        
        latencies.append((op_end - op_start) * 1000)
    
    duration = time.time() - start
    
    return BenchmarkResult(
        operation="GET",
        total_ops=num_ops,
        duration_sec=duration,
        ops_per_sec=num_ops / duration,
        latencies_ms=latencies
    )


def benchmark_mixed(client: MyDBClient, num_ops: int, read_ratio: float = 0.8) -> BenchmarkResult:
    latencies = []
    
    start = time.time()
    for i in range(num_ops):
        key = f"bench_key_{random.randint(0, num_ops):010d}"
        
        op_start = time.time()
        if random.random() < read_ratio:
            client.get(key)
        else:
            client.put(key, random_string(100))
        op_end = time.time()
        
        latencies.append((op_end - op_start) * 1000)
    
    duration = time.time() - start
    
    return BenchmarkResult(
        operation=f"MIXED ({int(read_ratio*100)}% reads)",
        total_ops=num_ops,
        duration_sec=duration,
        ops_per_sec=num_ops / duration,
        latencies_ms=latencies
    )


def print_result(result: BenchmarkResult):
    print(f"\n=== {result.operation} Benchmark ===")
    print(f"Total operations: {result.total_ops:,}")
    print(f"Duration: {result.duration_sec:.2f} seconds")
    print(f"Throughput: {result.ops_per_sec:,.0f} ops/sec")
    print(f"Latency (avg): {result.avg_latency_ms:.3f} ms")
    print(f"Latency (p50): {result.p50_latency_ms:.3f} ms")
    print(f"Latency (p99): {result.p99_latency_ms:.3f} ms")


def main():
    parser = argparse.ArgumentParser(description="MyDB Benchmark Tool")
    parser.add_argument("--host", default="127.0.0.1", help="Server host")
    parser.add_argument("--port", type=int, default=6379, help="Server port")
    parser.add_argument("--ops", type=int, default=10000, help="Number of operations")
    parser.add_argument("--key-size", type=int, default=16, help="Key size in bytes")
    parser.add_argument("--value-size", type=int, default=100, help="Value size in bytes")
    parser.add_argument("--type", choices=["put", "get", "mixed", "all"], default="all", help="Benchmark type")
    
    args = parser.parse_args()
    
    print(f"MyDB Benchmark")
    print(f"==============")
    print(f"Server: {args.host}:{args.port}")
    print(f"Operations: {args.ops:,}")
    print(f"Key size: {args.key_size} bytes")
    print(f"Value size: {args.value_size} bytes")
    
    client = MyDBClient(args.host, args.port)
    
    try:
        print("\nConnecting to server...")
        client.connect()
        
        if not client.ping():
            print("Failed to ping server")
            return 1
        
        print("Connected!")
        
        if args.type in ["put", "all"]:
            result = benchmark_puts(client, args.ops, args.key_size, args.value_size)
            print_result(result)
        
        if args.type in ["get", "all"]:
            result = benchmark_gets(client, args.ops)
            print_result(result)
        
        if args.type in ["mixed", "all"]:
            result = benchmark_mixed(client, args.ops)
            print_result(result)
        
        print("\nBenchmark complete!")
        
    except ConnectionRefusedError:
        print(f"Error: Could not connect to {args.host}:{args.port}")
        return 1
    except Exception as e:
        print(f"Error: {e}")
        return 1
    finally:
        client.close()
    
    return 0


if __name__ == "__main__":
    exit(main())
