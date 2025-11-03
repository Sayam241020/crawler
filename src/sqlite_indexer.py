#!/usr/bin/env python
"""
SQLite Indexers with Compression Support

Database storage for y=2 (DB1) per assignment
Supports ALL combinations:
- 3 index types (x): Boolean (1), TF (2), TF-IDF (3)
- 3 compression methods (z): None (1), Elias (2), zlib (3)

SQLite stores compressed postings as BLOB
Simple index on term column for fast lookups
Decompression happens in Python during query processing
"""

import json
import os
import sys
import sqlite3
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.self_indexer import SelfIndexer
from src.self_indexer_x2 import SelfIndexer_x2
from src.self_indexer_x3 import SelfIndexer_x3
from src.compression.elias import EliasCompressor
from src.compression.zlib_compressor import ZlibCompressor


class SQLiteIndexerBase:
    """
    Base class with common SQLite operations
    Supports compression for all index types
    """
    
    def __init__(self, compression_type='NONE'):
        self.db_path = None
        self.conn = None
        self.compr = compression_type
        
        # Select compressor
        if compression_type == 'CODE':
            self.compressor = EliasCompressor
        elif compression_type == 'CLIB':
            self.compressor = ZlibCompressor
        else:
            self.compressor = None
    
    def _connect(self, db_path: str):
        """Establish connection to SQLite database"""
        if self.conn is None:
            try:
                self.db_path = db_path
                self.conn = sqlite3.connect(db_path)
                self.conn.row_factory = sqlite3.Row
            except Exception as e:
                print(f"Error connecting to SQLite: {e}")
                raise
    
    def _close(self):
        """Close SQLite connection"""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def _create_tables(self, has_idf: bool = False):
        """
        Create tables for storing the inverted index
        Simple schema: term → compressed_data (BLOB)
        """
        cursor = self.conn.cursor()
        
        try:
            # Metadata table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            
            # Documents table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS documents (
                    doc_id TEXT PRIMARY KEY,
                    title TEXT,
                    metadata TEXT
                )
            """)
            
            # IDF scores table (only for x=3)
            if has_idf:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS idf (
                        term TEXT PRIMARY KEY,
                        idf_score REAL
                    )
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_idf_term ON idf(term)
                """)
            
            # Postings table - stores compressed data as BLOB
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS postings (
                    term TEXT PRIMARY KEY,
                    postings_data BLOB,
                    compression TEXT
                )
            """)
            
            # Simple index on term for fast lookup
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_postings_term ON postings(term)
            """)
            
            self.conn.commit()
            print(f"Created SQLite database: {self.db_path}")
            print(f"  - Compression: {self.compr}")
            print(f"  - IDF table: {has_idf}")
            
        except Exception as e:
            self.conn.rollback()
            print(f"Error creating tables: {e}")
            raise
    
    def _compress_postings(self, postings_list):
        """Compress postings list if compression is enabled"""
        if self.compressor:
            # Use compressor to compress the postings - returns bytes directly
            compressed_bytes = self.compressor.compress_postings_list(postings_list)
            # Store bytes directly as BLOB (no JSON encoding needed)
            return compressed_bytes
        else:
            # No compression, just JSON encode to bytes
            return json.dumps(postings_list).encode('utf-8')
    
    def _decompress_postings(self, compressed_data):
        """Decompress postings list if compression was used"""
        if self.compressor:
            # Decompress bytes directly
            return self.compressor.decompress_postings_list(compressed_data)
        else:
            # No compression, just decode JSON
            return json.loads(compressed_data.decode('utf-8'))


# ============================================================================
# Boolean Indexer (x=1)
# ============================================================================

class SQLiteIndexer_x1(SelfIndexer, SQLiteIndexerBase):
    """
    Boolean indexer with SQLite storage (y=2)
    Supports compression: z=1 (None), z=2 (Elias), z=3 (zlib)
    Identifier: SelfIndex_i1d2c{z}o0
    """
    
    def __init__(self, compression_type='NONE', optim='Null'):
        SQLiteIndexerBase.__init__(self, compression_type)
        SelfIndexer.__init__(self, dstore='DB1', compr=compression_type, optim=optim)
    
    def _save_index(self, index_id: str):
        """Save Boolean index to SQLite with optional compression"""
        # Create database in indices folder
        os.makedirs('indices', exist_ok=True)
        db_path = f"indices/{index_id}.db"
        
        self._connect(db_path)
        self._create_tables(has_idf=False)
        cursor = self.conn.cursor()
        
        print(f"Saving Boolean index to SQLite...")
        print(f"  Compression: {self.compr}")
        
        try:
            # Save metadata
            cursor.execute("""
                INSERT OR REPLACE INTO meta (key, value)
                VALUES (?, ?), (?, ?), (?, ?)
            """, ('identifier', index_id,
                  'compression', self.compr,
                  'index_type', 'Boolean'))
            
            # Save documents
            for doc_id, doc_info in self.documents.items():
                cursor.execute("""
                    INSERT OR REPLACE INTO documents (doc_id, title, metadata)
                    VALUES (?, ?, ?)
                """, (doc_id, doc_info.get('title', ''), json.dumps(doc_info)))
            
            # Save compressed postings
            postings_saved = 0
            for term, postings_list in self.inverted_index.items():
                compressed_data = self._compress_postings(postings_list)
                
                cursor.execute("""
                    INSERT OR REPLACE INTO postings (term, postings_data, compression)
                    VALUES (?, ?, ?)
                """, (term, compressed_data, self.compr))
                
                postings_saved += 1
                if postings_saved % 1000 == 0:
                    print(f"  Saved {postings_saved} terms...")
            
            self.conn.commit()
            print(f"✓ Saved {postings_saved} terms to SQLite")
            
            # Print file size
            file_size = os.path.getsize(db_path)
            print(f"Database size: {file_size / (1024*1024):.2f} MB")
            
        except Exception as e:
            self.conn.rollback()
            print(f"Error saving index: {e}")
            raise
        finally:
            self._close()
    
    def load_index(self, index_id: str):
        """Load Boolean index from SQLite with decompression"""
        db_path = f"indices/{index_id}.db"
        if not os.path.exists(db_path):
            db_path = f"{index_id}.db"
        
        self._connect(db_path)
        cursor = self.conn.cursor()
        
        print(f"--- Loading Boolean index from SQLite ---")
        
        try:
            # Load metadata
            cursor.execute("SELECT value FROM meta WHERE key = 'compression'")
            result = cursor.fetchone()
            if result:
                self.compr = result[0]
                print(f"  Compression: {self.compr}")
                # Re-initialize compressor
                if self.compr == 'CODE':
                    self.compressor = EliasCompressor
                elif self.compr == 'CLIB':
                    self.compressor = ZlibCompressor
            
            # Load documents
            cursor.execute("SELECT doc_id, metadata FROM documents")
            self.documents = {}
            for row in cursor.fetchall():
                doc_id, metadata_str = row
                self.documents[doc_id] = json.loads(metadata_str)
            
            # Load and decompress postings
            cursor.execute("SELECT term, postings_data FROM postings")
            self.inverted_index = defaultdict(list)
            
            for row in cursor.fetchall():
                term, compressed_data = row
                postings_list = self._decompress_postings(compressed_data)
                self.inverted_index[term] = postings_list
            
            print(f"✓ Loaded {len(self.inverted_index)} terms from SQLite")
            
        except Exception as e:
            print(f"Error loading index: {e}")
            raise
        finally:
            self._close()


# ============================================================================
# TF Indexer (x=2)
# ============================================================================

class SQLiteIndexer_x2(SelfIndexer_x2, SQLiteIndexerBase):
    """
    TF indexer with SQLite storage (y=2)
    Supports compression: z=1 (None), z=2 (Elias), z=3 (zlib)
    Identifier: SelfIndex_i2d2c{z}o0
    """
    
    def __init__(self, compression_type='NONE', optim='Null'):
        SQLiteIndexerBase.__init__(self, compression_type)
        SelfIndexer_x2.__init__(self, dstore='DB1', compr=compression_type, optim=optim)
        SQLiteIndexerBase.__init__(self, compression_type)
        SelfIndexer_x2.__init__(self)
        self.datastore = 'SQLite'
        # Restore compression type after SelfIndexer_x2.__init__() overwrites it
        self.compr = compression_type
        if compression_type == 'CODE':
            self.compressor = EliasCompressor
        elif compression_type == 'CLIB':
            self.compressor = ZlibCompressor
    
    def _save_index(self, index_id: str):
        """Save TF index to SQLite with optional compression"""
        os.makedirs('indices', exist_ok=True)
        db_path = f"indices/{index_id}.db"
        
        self._connect(db_path)
        self._create_tables(has_idf=False)
        cursor = self.conn.cursor()
        
        print(f"Saving TF index to SQLite...")
        print(f"  Compression: {self.compr}")
        
        try:
            # Save metadata
            cursor.execute("""
                INSERT OR REPLACE INTO meta (key, value)
                VALUES (?, ?), (?, ?), (?, ?)
            """, ('identifier', index_id,
                  'compression', self.compr,
                  'index_type', 'TF'))
            
            # Save documents
            for doc_id, doc_info in self.documents.items():
                cursor.execute("""
                    INSERT OR REPLACE INTO documents (doc_id, title, metadata)
                    VALUES (?, ?, ?)
                """, (doc_id, doc_info.get('title', ''), json.dumps(doc_info)))
            
            # Save compressed postings
            postings_saved = 0
            for term, postings_list in self.inverted_index.items():
                compressed_data = self._compress_postings(postings_list)
                
                cursor.execute("""
                    INSERT OR REPLACE INTO postings (term, postings_data, compression)
                    VALUES (?, ?, ?)
                """, (term, compressed_data, self.compr))
                
                postings_saved += 1
                if postings_saved % 1000 == 0:
                    print(f"  Saved {postings_saved} terms...")
            
            self.conn.commit()
            print(f"✓ Saved {postings_saved} terms to SQLite")
            
            # Print file size
            file_size = os.path.getsize(db_path)
            print(f"Database size: {file_size / (1024*1024):.2f} MB")
            
        except Exception as e:
            self.conn.rollback()
            print(f"Error saving index: {e}")
            raise
        finally:
            self._close()
    
    def load_index(self, index_id: str):
        """Load TF index from SQLite with decompression"""
        db_path = f"indices/{index_id}.db"
        if not os.path.exists(db_path):
            db_path = f"{index_id}.db"
        
        self._connect(db_path)
        cursor = self.conn.cursor()
        
        print(f"--- Loading TF index from SQLite ---")
        
        try:
            # Load metadata
            cursor.execute("SELECT value FROM meta WHERE key = 'compression'")
            result = cursor.fetchone()
            if result:
                self.compr = result[0]
                print(f"  Compression: {self.compr}")
                # Re-initialize compressor
                if self.compr == 'CODE':
                    self.compressor = EliasCompressor
                elif self.compr == 'CLIB':
                    self.compressor = ZlibCompressor
            
            # Load documents
            cursor.execute("SELECT doc_id, metadata FROM documents")
            self.documents = {}
            for row in cursor.fetchall():
                doc_id, metadata_str = row
                self.documents[doc_id] = json.loads(metadata_str)
            
            # Load and decompress postings
            cursor.execute("SELECT term, postings_data FROM postings")
            self.inverted_index = defaultdict(list)
            
            for row in cursor.fetchall():
                term, compressed_data = row
                postings_list = self._decompress_postings(compressed_data)
                self.inverted_index[term] = postings_list
            
            print(f"✓ Loaded {len(self.inverted_index)} terms from SQLite")
            
        except Exception as e:
            print(f"Error loading index: {e}")
            raise
        finally:
            self._close()


# ============================================================================
# TF-IDF Indexer (x=3)
# ============================================================================

class SQLiteIndexer_x3(SelfIndexer_x3, SQLiteIndexerBase):
    """
    TF-IDF indexer with SQLite storage (y=2)
    Supports compression: z=1 (None), z=2 (Elias), z=3 (zlib)
    Identifier: SelfIndex_i3d2c{z}o0
    """
    
    def __init__(self, compression_type='NONE', optim='Null'):
        SQLiteIndexerBase.__init__(self, compression_type)
        SelfIndexer_x3.__init__(self, dstore='DB1', compr=compression_type, optim=optim)
    
    def _save_index(self, index_id: str):
        """Save TF-IDF index to SQLite with optional compression"""
        os.makedirs('indices', exist_ok=True)
        db_path = f"indices/{index_id}.db"
        
        self._connect(db_path)
        self._create_tables(has_idf=True)
        cursor = self.conn.cursor()
        
        print(f"Saving TF-IDF index to SQLite...")
        print(f"  Compression: {self.compr}")
        
        try:
            # Save metadata
            cursor.execute("""
                INSERT OR REPLACE INTO meta (key, value)
                VALUES (?, ?), (?, ?), (?, ?)
            """, ('identifier', index_id,
                  'compression', self.compr,
                  'index_type', 'TF-IDF'))
            
            # Save documents
            for doc_id, doc_info in self.documents.items():
                cursor.execute("""
                    INSERT OR REPLACE INTO documents (doc_id, title, metadata)
                    VALUES (?, ?, ?)
                """, (doc_id, doc_info.get('title', ''), json.dumps(doc_info)))
            
            # Save IDF scores
            for term, idf_score in self.idf_scores.items():
                cursor.execute("""
                    INSERT OR REPLACE INTO idf (term, idf_score)
                    VALUES (?, ?)
                """, (term, idf_score))
            
            # Save compressed postings
            postings_saved = 0
            for term, postings_list in self.inverted_index.items():
                compressed_data = self._compress_postings(postings_list)
                
                cursor.execute("""
                    INSERT OR REPLACE INTO postings (term, postings_data, compression)
                    VALUES (?, ?, ?)
                """, (term, compressed_data, self.compr))
                
                postings_saved += 1
                if postings_saved % 1000 == 0:
                    print(f"  Saved {postings_saved} terms...")
            
            self.conn.commit()
            print(f"✓ Saved {postings_saved} terms to SQLite")
            
            # Print file size
            file_size = os.path.getsize(db_path)
            print(f"Database size: {file_size / (1024*1024):.2f} MB")
            
        except Exception as e:
            self.conn.rollback()
            print(f"Error saving index: {e}")
            raise
        finally:
            self._close()
    
    def load_index(self, index_id: str):
        """Load TF-IDF index from SQLite with decompression"""
        db_path = f"indices/{index_id}.db"
        if not os.path.exists(db_path):
            db_path = f"{index_id}.db"
        
        self._connect(db_path)
        cursor = self.conn.cursor()
        
        print(f"--- Loading TF-IDF index from SQLite ---")
        
        try:
            # Load metadata
            cursor.execute("SELECT value FROM meta WHERE key = 'compression'")
            result = cursor.fetchone()
            if result:
                self.compr = result[0]
                print(f"  Compression: {self.compr}")
                # Re-initialize compressor
                if self.compr == 'CODE':
                    self.compressor = EliasCompressor
                elif self.compr == 'CLIB':
                    self.compressor = ZlibCompressor
            
            # Load documents
            cursor.execute("SELECT doc_id, metadata FROM documents")
            self.documents = {}
            for row in cursor.fetchall():
                doc_id, metadata_str = row
                self.documents[doc_id] = json.loads(metadata_str)
            
            # Load IDF scores
            cursor.execute("SELECT term, idf_score FROM idf")
            self.idf_scores = {}
            for row in cursor.fetchall():
                term, idf_score = row
                self.idf_scores[term] = idf_score
            
            # Load and decompress postings
            cursor.execute("SELECT term, postings_data FROM postings")
            self.inverted_index = defaultdict(list)
            
            for row in cursor.fetchall():
                term, compressed_data = row
                postings_list = self._decompress_postings(compressed_data)
                self.inverted_index[term] = postings_list
            
            print(f"✓ Loaded {len(self.inverted_index)} terms from SQLite")
            
        except Exception as e:
            print(f"Error loading index: {e}")
            raise
        finally:
            self._close()


if __name__ == '__main__':
    print("="*80)
    print("SQLite Indexers with Compression Support")
    print("="*80)
    print("\nSupported combinations:")
    print("  x=1 (Boolean):  z=1 (None), z=2 (VByte), z=3 (zlib)")
    print("  x=2 (TF):       z=1 (None), z=2 (VByte), z=3 (zlib)")
    print("  x=3 (TF-IDF):   z=1 (None), z=2 (VByte), z=3 (zlib)")
    print("\nTotal: 9 combinations with SQLite")
    print("\nNote: SQLite is file-based (no Docker needed)")
