"""
Variable Byte (VByte) Encoding - Simple Integer Compression

VByte is a simple compression scheme for integers, commonly used in search engines.
Each integer is encoded using one or more bytes, where:
- The high bit (MSB) indicates continuation (0 = last byte, 1 = more bytes follow)
- The lower 7 bits store the actual data

This is particularly effective for compressing postings lists where:
- Document IDs are sorted and can use gap encoding
- Position lists have small deltas

Reference: "Introduction to Information Retrieval" - Manning, Raghavan, Schütze
"""

import json
from typing import List, Any


class VByteCompressor:
    """
    Variable Byte encoding for integer compression
    
    Used for compressing:
    - Document IDs (using gap encoding)
    - Position lists (using delta encoding)
    """
    
    @staticmethod
    def encode_number(n: int) -> bytes:
        """
        Encode a single integer using VByte encoding
        
        Args:
            n: Non-negative integer to encode
            
        Returns:
            bytes: Encoded representation
        """
        if n < 0:
            raise ValueError("VByte encoding only works with non-negative integers")
        
        bytes_list = []
        
        while True:
            # Take the lowest 7 bits
            bytes_list.insert(0, n % 128)
            
            if n < 128:
                break
            
            n //= 128
        
        # Set continuation bit on all bytes except the last
        for i in range(len(bytes_list) - 1):
            bytes_list[i] += 128
        
        return bytes(bytes_list)
    
    @staticmethod
    def decode_number(data: bytes, offset: int = 0) -> tuple[int, int]:
        """
        Decode a single integer from VByte encoding
        
        Args:
            data: Byte array containing encoded data
            offset: Starting position in the byte array
            
        Returns:
            tuple: (decoded_number, bytes_consumed)
        """
        n = 0
        bytes_read = 0
        
        while offset + bytes_read < len(data):
            byte = data[offset + bytes_read]
            bytes_read += 1
            
            # Check if this is the last byte (MSB = 0)
            if byte < 128:
                n = n * 128 + byte
                break
            else:
                n = n * 128 + (byte - 128)
        
        return n, bytes_read
    
    @staticmethod
    def encode_list(numbers: List[int], use_gap_encoding: bool = True) -> bytes:
        """
        Encode a list of integers using VByte with optional gap encoding
        
        Args:
            numbers: List of non-negative integers
            use_gap_encoding: If True, use gap encoding (for sorted lists like doc IDs)
                             If False, encode each number directly (for frequencies)
            
        Returns:
            bytes: Compressed representation
        """
        if not numbers:
            return b''
        
        result = []
        
        if use_gap_encoding:
            # Use gap encoding: store differences between consecutive values
            prev = 0
            for num in numbers:
                gap = num - prev
                result.append(VByteCompressor.encode_number(gap))
                prev = num
        else:
            # Direct encoding: store each number as-is
            for num in numbers:
                result.append(VByteCompressor.encode_number(num))
        
        return b''.join(result)
    
    @staticmethod
    def decode_list(data: bytes, use_gap_encoding: bool = True) -> List[int]:
        """
        Decode a list of integers from VByte encoding
        
        Args:
            data: Compressed byte array
            use_gap_encoding: If True, decode with gap encoding (for sorted lists)
                             If False, decode each number directly (for frequencies)
            
        Returns:
            List of decoded integers
        """
        if not data:
            return []
        
        result = []
        offset = 0
        
        if use_gap_encoding:
            # Decode with gap encoding
            cumulative = 0
            while offset < len(data):
                gap, bytes_read = VByteCompressor.decode_number(data, offset)
                cumulative += gap
                result.append(cumulative)
                offset += bytes_read
        else:
            # Direct decoding
            while offset < len(data):
                num, bytes_read = VByteCompressor.decode_number(data, offset)
                result.append(num)
                offset += bytes_read
        
        return result
    
    @staticmethod
    def compress_postings_list(postings: List[List[Any]]) -> bytes:
        """
        Compress a postings list in the format: [[doc_id, [positions]], ...]
        
        Args:
            postings: List of [doc_id, positions] pairs
            
        Returns:
            bytes: Compressed representation
        """
        import struct
        
        if not postings:
            return b''
        
        # Extract doc_ids, term_freq (if present), and positions
        doc_ids = []
        term_freqs = []
        all_positions = []
        has_term_freq = False
        
        for posting in postings:
            doc_id = posting[0]
            
            # Check format: [doc_id, positions] or [doc_id, term_freq, positions]
            if len(posting) == 2:
                # Format: [doc_id, positions]
                positions = posting[1] if isinstance(posting[1], list) else []
                term_freq = 0
            elif len(posting) == 3:
                # Format: [doc_id, term_freq, positions]
                term_freq = posting[1]
                positions = posting[2] if isinstance(posting[2], list) else []
                has_term_freq = True
            else:
                positions = []
                term_freq = 0
            
            # Doc IDs can be strings (e.g., "wiki_123"), store as-is (no compression)
            doc_ids.append(doc_id)
            term_freqs.append(term_freq)
            all_positions.append(positions)
        
        # Doc IDs are stored as JSON (can be strings like "wiki_123")
        doc_ids_json = json.dumps(doc_ids).encode('utf-8')
        
        # Compress term frequencies if present (use direct encoding, not gap encoding)
        term_freqs_compressed = b''
        if has_term_freq:
            term_freqs_compressed = VByteCompressor.encode_list(term_freqs, use_gap_encoding=False)
        
        # Compress each position list
        positions_compressed = []
        for positions in all_positions:
            if positions:
                pos_bytes = VByteCompressor.encode_list(positions)
                # Store length + data
                positions_compressed.append(struct.pack('I', len(pos_bytes)) + pos_bytes)
            else:
                positions_compressed.append(struct.pack('I', 0))
        
        # Format: 
        # [num_postings (4 bytes)][has_term_freq (1 byte)]
        # [doc_ids_length (4 bytes)][doc_ids (JSON)]
        # [term_freqs_length (4 bytes)][term_freqs] (if has_term_freq)
        # [positions...]
        result = struct.pack('I', len(postings))
        result += struct.pack('B', 1 if has_term_freq else 0)
        result += struct.pack('I', len(doc_ids_json))
        result += doc_ids_json
        
        if has_term_freq:
            result += struct.pack('I', len(term_freqs_compressed))
            result += term_freqs_compressed
            
        result += b''.join(positions_compressed)
        
        return result
    
    @staticmethod
    def decompress_postings_list(data: bytes) -> List[List[Any]]:
        """
        Decompress a postings list
        
        Args:
            data: Compressed byte array
            
        Returns:
            List of [doc_id, positions] pairs
        """
        import struct
        
        if not data or len(data) < 9:
            return []
        
        offset = 0
        
        # Read number of postings
        num_postings = struct.unpack('I', data[offset:offset+4])[0]
        offset += 4
        
        # Read has_term_freq flag
        has_term_freq = struct.unpack('B', data[offset:offset+1])[0] == 1
        offset += 1
        
        # Read doc IDs length
        doc_ids_length = struct.unpack('I', data[offset:offset+4])[0]
        offset += 4
        
        # Decompress doc IDs (stored as JSON)
        doc_ids_data = data[offset:offset+doc_ids_length]
        doc_ids = json.loads(doc_ids_data.decode('utf-8'))
        offset += doc_ids_length
        
        # Decompress term frequencies if present (use direct decoding, not gap decoding)
        term_freqs = []
        if has_term_freq:
            term_freqs_length = struct.unpack('I', data[offset:offset+4])[0]
            offset += 4
            term_freqs_data = data[offset:offset+term_freqs_length]
            term_freqs = VByteCompressor.decode_list(term_freqs_data, use_gap_encoding=False)
            offset += term_freqs_length
        
        # Decompress positions for each doc
        result = []
        for i, doc_id in enumerate(doc_ids):
            # Read positions length
            pos_length = struct.unpack('I', data[offset:offset+4])[0]
            offset += 4
            
            if pos_length > 0:
                # Decompress positions
                pos_data = data[offset:offset+pos_length]
                positions = VByteCompressor.decode_list(pos_data)
                offset += pos_length
            else:
                positions = []
            
            # Build posting based on format
            if has_term_freq:
                result.append([doc_id, term_freqs[i], positions])
            else:
                result.append([doc_id, positions])
        
        return result
    
    @staticmethod
    def compress_inverted_index(inverted_index: dict) -> dict:
        """
        Compress an entire inverted index
        
        Args:
            inverted_index: Dictionary of term -> postings list
            
        Returns:
            Dictionary of term -> compressed bytes (as base64 string for JSON)
        """
        import base64
        
        compressed = {}
        for term, postings in inverted_index.items():
            compressed_bytes = VByteCompressor.compress_postings_list(postings)
            # Convert to base64 for JSON serialization
            compressed[term] = base64.b64encode(compressed_bytes).decode('ascii')
        
        return compressed
    
    @staticmethod
    def decompress_inverted_index(compressed_index: dict) -> dict:
        """
        Decompress an entire inverted index
        
        Args:
            compressed_index: Dictionary of term -> compressed bytes (base64)
            
        Returns:
            Dictionary of term -> postings list
        """
        import base64
        
        decompressed = {}
        for term, compressed_str in compressed_index.items():
            compressed_bytes = base64.b64decode(compressed_str)
            postings = VByteCompressor.decompress_postings_list(compressed_bytes)
            decompressed[term] = postings
        
        return decompressed


# Example usage and testing
if __name__ == '__main__':
    # Test single number encoding/decoding
    print("=== Testing VByte Encoding ===\n")
    
    test_numbers = [0, 1, 127, 128, 255, 256, 1000, 100000]
    
    for num in test_numbers:
        encoded = VByteCompressor.encode_number(num)
        decoded, _ = VByteCompressor.decode_number(encoded)
        print(f"Number: {num:6d} | Encoded: {len(encoded)} bytes | Decoded: {decoded} | Match: {num == decoded}")
    
    # Test list encoding with gap encoding
    print("\n=== Testing List Encoding ===\n")
    
    doc_ids = [10, 15, 20, 100, 101, 102, 1000]
    encoded = VByteCompressor.encode_list(doc_ids)
    decoded = VByteCompressor.decode_list(encoded)
    
    print(f"Original: {doc_ids}")
    print(f"Compressed: {len(encoded)} bytes")
    print(f"Decoded: {decoded}")
    print(f"Match: {doc_ids == decoded}")
    
    # Test postings list compression
    print("\n=== Testing Postings List Compression ===\n")
    
    postings = [
        [1, [0, 5, 10]],
        [3, [2, 7, 15, 20]],
        [5, [1, 3]],
        [100, [0, 1, 2, 3, 4]]
    ]
    
    compressed = VByteCompressor.compress_postings_list(postings)
    decompressed = VByteCompressor.decompress_postings_list(compressed)
    
    print(f"Original postings: {postings}")
    print(f"Compressed: {len(compressed)} bytes")
    print(f"Decompressed: {decompressed}")
    print(f"Match: {postings == decompressed}")
    
    # Calculate compression ratio
    import json
    original_json = json.dumps(postings)
    ratio = len(original_json) / len(compressed)
    print(f"\nCompression ratio: {ratio:.2f}x (JSON: {len(original_json)} bytes → VByte: {len(compressed)} bytes)")
