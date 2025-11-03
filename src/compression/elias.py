"""
Elias Gamma and Delta Encoding for Inverted Index Compression

This module implements Elias encoding schemes for compressing posting lists
in information retrieval systems. No external compression libraries used.

Reference:
- Elias, P. (1975). "Universal codeword sets and representations of the integers"

Author: Information Retrieval Assignment 1
Date: November 2, 2025
"""

import base64
from typing import List, Tuple


class EliasEncoder:
    """
    Elias Gamma and Delta encoding for integer compression.
    
    Elias Gamma: Optimal for small numbers (1-100)
    Elias Delta: Better for larger numbers (100+)
    """
    
    @staticmethod
    def encode_gamma(n: int) -> str:
        """
        Encode a positive integer using Elias Gamma encoding.
        
        Algorithm:
            1. Find L = floor(log2(n)) + 1 (binary length)
            2. Write L-1 zeros as unary length prefix
            3. Write binary representation of n
        
        Args:
            n: Positive integer to encode (must be >= 1)
            
        Returns:
            Binary string (e.g., "0001101" for 13)
            
        Examples:
            encode_gamma(1)  -> "1"           (1 bit)
            encode_gamma(2)  -> "010"         (3 bits)
            encode_gamma(5)  -> "00101"       (5 bits)
            encode_gamma(13) -> "0001101"     (7 bits)
            
        Raises:
            ValueError: If n < 1
        """
        if n < 1:
            raise ValueError(f"Elias Gamma requires n >= 1, got {n}")
        
        # Get binary representation without '0b' prefix
        binary = bin(n)[2:]
        length = len(binary)
        
        # Unary prefix: (length - 1) zeros
        unary_prefix = '0' * (length - 1)
        
        # Result: unary prefix + binary representation
        return unary_prefix + binary
    
    @staticmethod
    def decode_gamma(bitstring: str, start_pos: int = 0) -> Tuple[int, int]:
        """
        Decode an Elias Gamma encoded number from a bitstring.
        
        Algorithm:
            1. Count leading zeros -> this is L-1
            2. Read next L bits -> this is the number
        
        Args:
            bitstring: Binary string containing encoded data
            start_pos: Starting position in bitstring
            
        Returns:
            Tuple of (decoded_number, next_position)
            
        Raises:
            ValueError: If bitstring is incomplete or invalid
        """
        if start_pos >= len(bitstring):
            raise ValueError("Start position beyond bitstring length")
        
        # Count leading zeros
        zero_count = 0
        pos = start_pos
        while pos < len(bitstring) and bitstring[pos] == '0':
            zero_count += 1
            pos += 1
        
        # Length of binary representation
        length = zero_count + 1
        
        # Read next 'length' bits
        if pos + length > len(bitstring):
            raise ValueError("Incomplete Elias Gamma code")
        
        binary_str = bitstring[pos:pos + length]
        number = int(binary_str, 2)
        
        return number, pos + length
    
    @staticmethod
    def encode_delta(n: int) -> str:
        """
        Encode a positive integer using Elias Delta encoding.
        
        Algorithm:
            1. Find L = floor(log2(n)) + 1
            2. Encode L using Gamma encoding
            3. Write last L-1 bits of n (omit leading 1)
        
        Args:
            n: Positive integer to encode (must be >= 1)
            
        Returns:
            Binary string
            
        Examples:
            encode_delta(1)   -> "1"          (1 bit)
            encode_delta(2)   -> "0100"       (4 bits)
            encode_delta(13)  -> "00100101"   (8 bits)
            encode_delta(100) -> "001011100100" (12 bits)
        
        Note: 
            Delta is more efficient than Gamma for larger numbers (n > 15)
            
        Raises:
            ValueError: If n < 1
        """
        if n < 1:
            raise ValueError(f"Elias Delta requires n >= 1, got {n}")
        
        # Get binary representation
        binary = bin(n)[2:]
        length = len(binary)
        
        # Encode length using Gamma
        length_code = EliasEncoder.encode_gamma(length)
        
        # Get last (length - 1) bits of n (all except leading 1)
        remainder = binary[1:] if length > 1 else ''
        
        return length_code + remainder
    
    @staticmethod
    def decode_delta(bitstring: str, start_pos: int = 0) -> Tuple[int, int]:
        """
        Decode an Elias Delta encoded number from a bitstring.
        
        Args:
            bitstring: Binary string containing encoded data
            start_pos: Starting position in bitstring
            
        Returns:
            Tuple of (decoded_number, next_position)
            
        Raises:
            ValueError: If bitstring is incomplete or invalid
        """
        if start_pos >= len(bitstring):
            raise ValueError("Start position beyond bitstring length")
        
        # Decode length using Gamma
        length, pos = EliasEncoder.decode_gamma(bitstring, start_pos)
        
        if length == 1:
            # Special case: number is 1
            return 1, pos
        
        # Read next (length - 1) bits
        remainder_bits = length - 1
        if pos + remainder_bits > len(bitstring):
            raise ValueError("Incomplete Elias Delta code")
        
        # Reconstruct number: '1' + remainder
        remainder = bitstring[pos:pos + remainder_bits]
        binary_str = '1' + remainder
        number = int(binary_str, 2)
        
        return number, pos + remainder_bits


class EliasCompressor:
    """
    Posting list compressor using adaptive Elias encoding.
    
    Strategy:
    - Delta-encode document IDs (store gaps, not absolute IDs)
    - Use Gamma for small values (TF, small gaps)
    - Use Delta for large values (large gaps)
    - Delta-encode positions within each document
    
    This adaptive approach gives optimal compression for typical IR workloads.
    """
    
    # Threshold for choosing Gamma vs Delta (tuned for our data)
    GAMMA_THRESHOLD = 15  # Use Gamma for n <= 15, Delta for n > 15
    
    @staticmethod
    def compress_postings(postings_list: List) -> str:
        """
        Compress a posting list using adaptive Elias encoding.
        
        Input format: 
            [[doc_id, tf, [pos1, pos2, ...]], ...]
            
        Output: 
            Base64-encoded compressed bitstring
        
        Compression strategy:
            - Document ID gaps: Adaptive (Gamma/Delta based on gap size)
            - Term frequencies: Gamma (usually 1-10)
            - Positions: Delta-encoded, then Gamma/Delta adaptive
        
        Args:
            postings_list: List of [doc_id_str, tf, positions]
            
        Returns:
            Base64-encoded compressed data
            
        Example:
            postings = [["news_1", 3, [10, 25, 89]], ["news_5", 2, [5, 67]]]
            compressed = EliasCompressor.compress_postings(postings)
        """
        if not postings_list:
            return ""
        
        # CRITICAL: Deduplicate and sort postings by doc_id number
        # 1. Remove duplicates by keeping only the last occurrence (most recent TF/positions)
        # 2. Sort by numeric doc_id for delta encoding
        seen_docs = {}
        for posting in postings_list:
            doc_id_str = posting[0]
            seen_docs[doc_id_str] = posting  # Keep last occurrence
        
        # DEBUG: Check if deduplication worked
        original_count = len(postings_list)
        deduplicated_count = len(seen_docs)
        if original_count != deduplicated_count:
            print(f"  [DEDUP] Removed {original_count - deduplicated_count} duplicates from postings")
        
        postings_list = sorted(seen_docs.values(), 
                              key=lambda p: EliasCompressor._doc_id_to_number(p[0]))
        
        bitstring = ""
        
        # Encode number of postings (Gamma - usually small)
        bitstring += EliasEncoder.encode_gamma(len(postings_list))
        
        # CRITICAL: Start at -1 to handle doc_id that converts to 0
        # (similar fix to position encoding)
        prev_doc_num = -1
        
        for posting in postings_list:
            doc_id_str, tf, positions = posting
            
            # Convert doc_id string to number
            doc_num = EliasCompressor._doc_id_to_number(doc_id_str)
            
            # Delta encode doc_id (gap from previous)
            gap = doc_num - prev_doc_num
            if gap < 1:
                # DEBUG: Show actual doc_ids causing the issue
                prev_doc_id = "START"
                for p in postings_list:
                    if EliasCompressor._doc_id_to_number(p[0]) == prev_doc_num:
                        prev_doc_id = p[0]
                        break
                raise ValueError(
                    f"Doc IDs must be sorted! {prev_doc_num} -> {doc_num}\n"
                    f"  Previous doc_id: {prev_doc_id}\n"
                    f"  Current doc_id: {doc_id_str}\n"
                    f"  Total postings in this term: {len(postings_list)}"
                )
            
            # Adaptive encoding: Gamma for small gaps, Delta for large
            # Add flag bit: 0 = Gamma, 1 = Delta
            if gap <= EliasCompressor.GAMMA_THRESHOLD:
                bitstring += '0'  # Flag: Gamma
                bitstring += EliasEncoder.encode_gamma(gap)
            else:
                bitstring += '1'  # Flag: Delta
                bitstring += EliasEncoder.encode_delta(gap)
            
            prev_doc_num = doc_num
            
            # Encode TF (usually small: 1-50, so use Gamma)
            bitstring += EliasEncoder.encode_gamma(tf)
            
            # Deduplicate and sort positions (in case of preprocessing issues)
            positions = sorted(set(positions))
            
            # Encode number of positions
            bitstring += EliasEncoder.encode_gamma(len(positions))
            
            # Delta encode positions (sequential within document)
            # Start at -1 so first position (which can be 0) has gap of at least 1
            prev_pos = -1
            for pos in positions:
                pos_gap = pos - prev_pos
                if pos_gap < 1:
                    raise ValueError(f"Positions must be sorted and unique! {prev_pos} -> {pos}")
                
                # Adaptive encoding for position gaps with flag bit
                if pos_gap <= EliasCompressor.GAMMA_THRESHOLD:
                    bitstring += '0'  # Flag: Gamma
                    bitstring += EliasEncoder.encode_gamma(pos_gap)
                else:
                    bitstring += '1'  # Flag: Delta
                    bitstring += EliasEncoder.encode_delta(pos_gap)
                
                prev_pos = pos
        
        # Convert bitstring to bytes and encode as base64
        return EliasCompressor._bitstring_to_base64(bitstring)
    
    @staticmethod
    def decompress_postings(compressed_base64: str) -> List:
        """
        Decompress a posting list from Elias-encoded base64.
        
        Input: 
            Base64-encoded compressed bitstring
            
        Output: 
            [[doc_id, tf, [pos1, pos2, ...]], ...]
        
        Args:
            compressed_base64: Base64 string from compress_postings()
            
        Returns:
            Decompressed posting list
            
        Example:
            compressed = "eJyNVMs..."
            postings = EliasCompressor.decompress_postings(compressed)
        """
        if not compressed_base64:
            return []
        
        # Decode base64 to bitstring
        bitstring = EliasCompressor._base64_to_bitstring(compressed_base64)
        
        postings_list = []
        pos = 0
        
        # Decode number of postings
        num_postings, pos = EliasEncoder.decode_gamma(bitstring, pos)
        
        # CRITICAL: Start at -1 to match compression (handles doc_id 0)
        prev_doc_num = -1
        
        for _ in range(num_postings):
            # Decode doc_id gap
            # Read flag bit: 0 = Gamma, 1 = Delta
            if pos >= len(bitstring):
                raise ValueError("Incomplete data: expected flag bit for doc_id gap")
            
            flag = bitstring[pos]
            pos += 1
            
            if flag == '0':
                # Gamma encoded
                gap, pos = EliasEncoder.decode_gamma(bitstring, pos)
            else:
                # Delta encoded
                gap, pos = EliasEncoder.decode_delta(bitstring, pos)
            
            doc_num = prev_doc_num + gap
            prev_doc_num = doc_num
            
            # Decode TF (always Gamma)
            tf, pos = EliasEncoder.decode_gamma(bitstring, pos)
            
            # Decode number of positions
            num_positions, pos = EliasEncoder.decode_gamma(bitstring, pos)
            
            # Decode positions (start at -1 to match compression)
            positions = []
            prev_position = -1
            for _ in range(num_positions):
                # Read flag bit for position gap
                if pos >= len(bitstring):
                    raise ValueError("Incomplete data: expected flag bit for position gap")
                
                flag = bitstring[pos]
                pos += 1
                
                if flag == '0':
                    # Gamma encoded
                    pos_gap, pos = EliasEncoder.decode_gamma(bitstring, pos)
                else:
                    # Delta encoded
                    pos_gap, pos = EliasEncoder.decode_delta(bitstring, pos)
                
                position = prev_position + pos_gap
                positions.append(position)
                prev_position = position
            
            # Convert doc_num back to doc_id string
            doc_id_str = EliasCompressor._number_to_doc_id(doc_num)
            
            postings_list.append([doc_id_str, tf, positions])
        
        return postings_list
    
    @staticmethod
    def _doc_id_to_number(doc_id_str: str) -> int:
        """
        Convert doc_id string to number for compression.
        
        Strategy: Use numeric offset to distinguish news from wiki
        - news_X  -> X
        - wiki_Y  -> 100000 + Y
        
        This ensures all doc IDs are unique numbers that can be sorted.
        """
        parts = doc_id_str.split('_')
        if len(parts) != 2:
            raise ValueError(f"Invalid doc_id format: {doc_id_str}")
        
        prefix, num_str = parts
        num = int(num_str)
        
        # Offset to distinguish news from wiki
        if prefix == 'news':
            return num
        elif prefix == 'wiki':
            return 100000 + num
        else:
            raise ValueError(f"Unknown doc_id prefix: {prefix}")
    
    @staticmethod
    def _number_to_doc_id(num: int) -> str:
        """
        Convert number back to doc_id string.
        
        Inverse of _doc_id_to_number()
        """
        if num < 100000:
            return f"news_{num}"
        else:
            return f"wiki_{num - 100000}"
    
    @staticmethod
    def _bitstring_to_base64(bitstring: str) -> str:
        """
        Convert binary string to base64 for JSON storage.
        
        Process:
        1. Pad bitstring to multiple of 8 bits
        2. Convert to bytes
        3. Encode as base64
        """
        # Pad to multiple of 8 bits
        padding = (8 - len(bitstring) % 8) % 8
        bitstring += '0' * padding
        
        # Convert to bytes
        byte_array = bytearray()
        for i in range(0, len(bitstring), 8):
            byte = int(bitstring[i:i+8], 2)
            byte_array.append(byte)
        
        # Encode as base64
        return base64.b64encode(bytes(byte_array)).decode('utf-8')
    
    @staticmethod
    def _base64_to_bitstring(base64_str: str) -> str:
        """
        Convert base64 back to binary string.
        
        Inverse of _bitstring_to_base64()
        """
        # Decode base64
        byte_data = base64.b64decode(base64_str)
        
        # Convert to bitstring
        bitstring = ''.join(format(byte, '08b') for byte in byte_data)
        
        return bitstring
    
    @staticmethod
    def compress_inverted_index(inverted_index: dict) -> dict:
        """
        Compress an entire inverted index.
        
        Args:
            inverted_index: Dictionary of term -> postings list
            
        Returns:
            Dictionary of term -> compressed base64 string
        """
        compressed = {}
        
        for term, postings in inverted_index.items():
            compressed[term] = EliasCompressor.compress_postings(postings)
        
        return compressed
    
    @staticmethod
    def decompress_inverted_index(compressed_index: dict) -> dict:
        """
        Decompress an entire inverted index.
        
        Args:
            compressed_index: Dictionary of term -> compressed base64 string
            
        Returns:
            Dictionary of term -> postings list
        """
        decompressed = {}
        
        for term, compressed_str in compressed_index.items():
            decompressed[term] = EliasCompressor.decompress_postings(compressed_str)
        
        return decompressed


# Self-test and examples
if __name__ == "__main__":
    print("=" * 70)
    print("ELIAS ENCODING IMPLEMENTATION - SELF TEST")
    print("=" * 70)
    
    # Test 1: Gamma Encoding
    print("\n[TEST 1] Elias Gamma Encoding")
    print("-" * 70)
    test_numbers = [1, 2, 3, 5, 10, 13, 15]
    for n in test_numbers:
        encoded = EliasEncoder.encode_gamma(n)
        decoded, _ = EliasEncoder.decode_gamma(encoded)
        saved_bits = 32 - len(encoded)
        print(f"  Gamma({n:3d}) = {encoded:15s} ({len(encoded):2d} bits, saved {saved_bits:2d}) -> {decoded}")
        assert n == decoded, f"Gamma decode failed for {n}"
    
    # Test 2: Delta Encoding
    print("\n[TEST 2] Elias Delta Encoding")
    print("-" * 70)
    test_numbers = [1, 2, 13, 50, 100, 1000]
    for n in test_numbers:
        encoded = EliasEncoder.encode_delta(n)
        decoded, _ = EliasEncoder.decode_delta(encoded)
        saved_bits = 32 - len(encoded)
        print(f"  Delta({n:4d}) = {encoded:20s} ({len(encoded):2d} bits, saved {saved_bits:2d}) -> {decoded}")
        assert n == decoded, f"Delta decode failed for {n}"
    
    # Test 3: Gamma vs Delta Comparison
    print("\n[TEST 3] Gamma vs Delta - When to Use Which?")
    print("-" * 70)
    test_numbers = [5, 10, 15, 20, 50, 100]
    for n in test_numbers:
        gamma = EliasEncoder.encode_gamma(n)
        delta = EliasEncoder.encode_delta(n)
        better = "Gamma" if len(gamma) <= len(delta) else "Delta"
        print(f"  n={n:3d}: Gamma={len(gamma):2d} bits, Delta={len(delta):2d} bits -> {better} is better")
    
    # Test 4: Posting List Compression
    print("\n[TEST 4] Posting List Compression")
    print("-" * 70)
    test_postings = [
        ["news_1", 3, [10, 25, 89]],
        ["news_5", 2, [5, 67]],
        ["news_10", 1, [42]],
        ["wiki_100", 5, [1, 3, 5, 7, 9]]
    ]
    
    print(f"  Original postings:")
    for p in test_postings:
        print(f"    {p}")
    
    compressed = EliasCompressor.compress_postings(test_postings)
    print(f"\n  Compressed (base64): {compressed[:50]}...")
    print(f"  Compressed length: {len(compressed)} characters")
    
    decompressed = EliasCompressor.decompress_postings(compressed)
    print(f"\n  Decompressed postings:")
    for p in decompressed:
        print(f"    {p}")
    
    match = test_postings == decompressed
    print(f"\n  Compression/Decompression: {'✓ PASS' if match else '✗ FAIL'}")
    assert match, "Posting list compression failed!"
    
    # Test 5: Compression Ratio Estimate
    print("\n[TEST 5] Compression Ratio Estimate")
    print("-" * 70)
    import json
    original_json = json.dumps(test_postings)
    original_bytes = len(original_json.encode('utf-8'))
    compressed_bytes = len(compressed.encode('utf-8'))
    ratio = original_bytes / compressed_bytes if compressed_bytes > 0 else 0
    
    print(f"  Original JSON size: {original_bytes} bytes")
    print(f"  Compressed size: {compressed_bytes} bytes")
    print(f"  Compression ratio: {ratio:.2f}x")
    print(f"  Space saved: {100 * (1 - compressed_bytes/original_bytes):.1f}%")
    
    print("\n" + "=" * 70)
    print("ALL TESTS PASSED ✓")
    print("=" * 70)
