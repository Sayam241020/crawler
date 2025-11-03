import json
import os
from collections import defaultdict
from .index_base import IndexBase
from .preprocessor import preprocess_text
from typing import Iterable, Dict, List, Set
import re

class SelfIndexer(IndexBase):
    def __init__(self, dstore='CUSTOM', compr='NONE', optim='Null'):
        super().__init__(core='SelfIndex', info='BOOLEAN', dstore=dstore, 
                         qproc='TERMatat', compr=compr, optim=optim)
        self.inverted_index = defaultdict(list)
        self.documents = {}
        self.precedence = {'NOT': 3, 'AND': 2, 'OR': 1}
        self.compression_type = compr
        self.optim = optim  # Store optimization type
        
        # Skip pointers support (build-time when optim='Skipping')
        self.skip_pointers_enabled = False
        self.skip_index = None

    def create_index(self, index_id: str, documents: Iterable[Dict]):
        print(f"--- Building '{self.identifier_short}' from scratch ---")
        
        doc_count = 0
        for doc in documents:
            doc_id = doc['doc_id']
            self.documents[doc_id] = {'title': doc.get('title', 'No Title')}
            
            # Check if document has preprocessed tokens (FAST PATH!)
            if 'tokens' in doc:
                tokens = doc['tokens']  # Already preprocessed!
            else:
                # Fallback: preprocess from raw content (SLOW PATH)
                content = doc.get('content', '')
                tokens = preprocess_text(content)
            
            term_positions = defaultdict(list)
            for i, token in enumerate(tokens):
                term_positions[token].append(i)
            
            for term, positions in term_positions.items():
                self.inverted_index[term].append([doc_id, positions])
            doc_count += 1

        print(f"Index building complete. Processed {doc_count} documents.")
        print(f"Total unique terms: {len(self.inverted_index)}")
        
        # BUILD-TIME OPTIMIZATION: Add skip pointers if requested
        if self.optim == 'Skipping':
            print("\n--- Building Skip Pointers (Build-Time Optimization) ---")
            from .skip_pointer_builder import add_skip_pointers, verify_skip_pointers
            import time
            
            start = time.perf_counter()
            self.inverted_index = add_skip_pointers(dict(self.inverted_index))
            elapsed = time.perf_counter() - start
            
            # Verify skip pointers
            stats = verify_skip_pointers(self.inverted_index)
            print(f"\nSkip Pointer Statistics:")
            print(f"  Total terms: {stats['total_terms']}")
            print(f"  Total postings: {stats['total_postings']}")
            print(f"  Total skip pointers: {stats['total_skip_pointers']}")
            print(f"  Avg skip interval: {stats['avg_skip_interval']:.2f}")
            print(f"  Build time: {elapsed:.2f}s\n")
            
            self.skip_pointers_enabled = True
        
        self._save_index(index_id)

    def _save_index(self, index_id: str):
        # Ensure indices directory exists
        os.makedirs('indices', exist_ok=True)
        
        # Save in indices/ folder
        filename = f"indices/{self.identifier_short}.json"
        index_data = {
            "identifier": self.identifier_short,
            "inverted_index": self.inverted_index,
            "documents": self.documents,
            "skip_pointers_enabled": self.skip_pointers_enabled  # Save skip pointer flag
        }
        with open(filename, 'w') as f:
            json.dump(index_data, f)
        print(f"Index saved to disk as {filename}")

    def load_index(self, index_id: str):
        """
        Load index from disk.
        Automatically detects and handles compressed indices.
        """
        # Try indices/ folder first, then root
        filename = f"indices/{index_id}.json"
        if not os.path.exists(filename):
            filename = f"{index_id}.json"
        
        print(f"--- Loading index from {filename} ---")
        try:
            with open(filename, 'r') as f:
                index_data = json.load(f)
            
            # Check if index is compressed
            compression_type = index_data.get("compression", "NONE")
            
            if compression_type != "NONE" and compression_type in ["CODE", "CLIB"]:
                # Compressed index - need to decompress
                print(f"Detected compression: {compression_type}")
                print(f"Decompressing inverted index...")
                
                # Import compressor
                if compression_type == "CODE":
                    from src.compression.elias import EliasCompressor
                    compressor = EliasCompressor
                elif compression_type == "CLIB":
                    from src.compression.zlib_compressor import ZlibCompressor
                    compressor = ZlibCompressor
                
                # Decompress inverted index
                compressed_index = index_data["inverted_index"]
                self.inverted_index = defaultdict(
                    list,
                    compressor.decompress_inverted_index(compressed_index)
                )
            else:
                # Uncompressed index - load directly
                self.inverted_index = defaultdict(list, index_data["inverted_index"])
            
            self.documents = index_data["documents"]
            
            # Load skip pointer metadata if present
            self.skip_pointers_enabled = index_data.get("skip_pointers_enabled", False)
            if self.skip_pointers_enabled:
                print("Skip pointers detected in index (build-time optimization)")
            
            print(f"Index loaded successfully. Found {len(self.inverted_index)} terms.")
        except FileNotFoundError:
            print(f"Error: Index file {filename} not found. Please create it first.")
    
    def enable_skip_pointers(self):
        """
        Enable skip pointers for faster Boolean AND/OR operations.
        Generates skip pointers in-memory (no index rebuild needed).
        """
        if self.skip_pointers_enabled:
            print("Skip pointers already enabled.")
            return
        
        if not self.inverted_index:
            print("Error: Index not loaded. Load index first.")
            return
        
        print("Generating skip pointers...")
        from .skip_pointers import SkipPointerIndex
        import time
        
        start = time.perf_counter()
        self.skip_index = SkipPointerIndex(self.inverted_index)
        elapsed = time.perf_counter() - start
        
        self.skip_pointers_enabled = True
        self.optim = 'Skipping'  # Update optimization marker
        print(f"Skip pointers generation time: {elapsed:.2f}s")
    
    def disable_skip_pointers(self):
        """Disable skip pointers (use regular set operations)."""
        self.skip_pointers_enabled = False
        self.skip_index = None
        self.optim = 'Null'
        print("Skip pointers disabled.")

    # In src/self_indexer.py, inside the SelfIndexer class

    # --- Query Processing Methods ---
    def _tokenize_query(self, query_str: str) -> List[str]:
        """
        Tokenizes a boolean query string into terms and operators.
        Returns a list of tokens: quoted terms, operators (AND/OR/NOT/PHRASE), and parentheses.
        """
        # Regex pattern matches:
        # - Parentheses: ( or )
        # - Quoted strings: "anything"
        # - Boolean operators: AND, OR, NOT, PHRASE (case-insensitive)
        token_regex = r'(\(|\)|"[^"]+"|\b(?:PHRASE|AND|OR|NOT)\b)'
        tokens = re.findall(token_regex, query_str, re.IGNORECASE)
        
        # Standardize operators to uppercase, keep terms as-is (with quotes)
        standardized = []
        for token in tokens:
            upper_token = token.upper()
            if upper_token in ['PHRASE', 'AND', 'OR', 'NOT', '(', ')']:
                standardized.append(upper_token)
            else:
                standardized.append(token)  # Keep quoted term as-is
        
        return standardized

    def _get_postings(self, term: str) -> Set[str]:
        """
        Retrieves the set of doc_ids for a given query term.
        Applies the same preprocessing that was used during indexing.
        
        Handles both regular and skip-pointer-enhanced indices.
        """
        # Remove quotes from the term
        term = term.strip('"')
        
        # Apply the same preprocessing used during indexing
        processed_tokens = preprocess_text(term)
        
        # If preprocessing yields no tokens (e.g., stopword), return empty set
        if not processed_tokens:
            return set()
        
        # Use the first token (handles multi-word terms, though grammar expects single words)
        processed_term = processed_tokens[0]
        
        # Retrieve postings for this term
        postings = self.inverted_index.get(processed_term, [])
        
        # Handle skip-pointer-enhanced indices (build-time optimization)
        if self.skip_pointers_enabled and isinstance(postings, dict):
            # Skip pointer format: {'postings': [...], 'skip_interval': N}
            postings = postings.get('postings', [])
            # Extract doc_ids from enhanced format
            return {posting['doc_id'] for posting in postings}
        
        # Regular format: [[doc_id, positions], ...]
        # Extract and return the set of doc_ids
        return {posting[0] for posting in postings}
    
    def _get_postings_with_term(self, term: str):
        """
        Retrieves postings with the processed term name.
        Returns: (doc_id_set, processed_term, postings_list)
        Used for skip pointer optimization.
        
        Handles both regular and skip-pointer-enhanced indices.
        """
        # Remove quotes from the term
        term = term.strip('"')
        
        # Apply the same preprocessing used during indexing
        processed_tokens = preprocess_text(term)
        
        # If preprocessing yields no tokens, return empty
        if not processed_tokens:
            return (set(), None, [])
        
        # Use the first token
        processed_term = processed_tokens[0]
        
        # Retrieve postings for this term
        postings = self.inverted_index.get(processed_term, [])
        
        # Handle skip-pointer-enhanced indices
        if self.skip_pointers_enabled and isinstance(postings, dict):
            # Skip pointer format: {'postings': [...], 'skip_interval': N}
            enhanced_postings = postings.get('postings', [])
            doc_ids = {posting['doc_id'] for posting in enhanced_postings}
            return (doc_ids, processed_term, enhanced_postings)
        
        # Regular format: [[doc_id, positions], ...]
        # Return both set and postings list
        doc_ids = {posting[0] for posting in postings}
        return (doc_ids, processed_term, postings)
    
    def _get_postings_with_positions(self, term: str) -> Dict[str, List[int]]:
        """
        Retrieves doc_ids with their position lists for phrase queries.
        Returns: {doc_id: [positions]}
        
        Handles both regular and skip-pointer-enhanced indices.
        """
        term = term.strip('"')
        processed_tokens = preprocess_text(term)
        
        if not processed_tokens:
            return {}
        
        processed_term = processed_tokens[0]
        postings = self.inverted_index.get(processed_term, [])
        
        # Handle skip-pointer-enhanced indices
        if self.skip_pointers_enabled and isinstance(postings, dict):
            # Skip pointer format: {'postings': [{'doc_id': ..., 'positions': ...}], ...}
            postings = postings.get('postings', [])
            return {posting['doc_id']: posting['positions'] for posting in postings}
        
        # Regular format: [[doc_id, positions], ...]
        # Return dict mapping doc_id to positions
        return {posting[0]: posting[1] for posting in postings}
    
    def _check_phrase(self, terms: List[str]) -> Set[str]:
        """
        Checks for phrase match: terms must appear consecutively in order.
        Args:
            terms: List of quoted terms in phrase order
        Returns:
            Set of doc_ids containing the phrase
        """
        if not terms:
            return set()
        
        if len(terms) == 1:
            # Single term phrase = simple term search
            return self._get_postings(terms[0])
        
        # Get postings with positions for all terms
        term_postings = [self._get_postings_with_positions(term) for term in terms]
        
        # Find docs containing all terms
        common_docs = set(term_postings[0].keys())
        for posting in term_postings[1:]:
            common_docs &= set(posting.keys())
        
        # Check position adjacency in common docs
        matching_docs = set()
        for doc_id in common_docs:
            # Get positions for each term in this doc
            positions_lists = [term_postings[i][doc_id] for i in range(len(terms))]
            
            # Check if any position sequence matches the phrase
            first_term_positions = positions_lists[0]
            for start_pos in first_term_positions:
                # Check if subsequent terms appear at consecutive positions
                match = True
                for i in range(1, len(terms)):
                    expected_pos = start_pos + i
                    if expected_pos not in positions_lists[i]:
                        match = False
                        break
                
                if match:
                    matching_docs.add(doc_id)
                    break  # Found a match in this doc, move to next doc
        
        return matching_docs

    def _preprocess_phrase_queries(self, tokens: List[str]) -> List[str]:
        """
        Preprocesses tokens to convert PHRASE syntax into a special token.
        
        Supports TWO formats:
        1. Natural format: PHRASE "multi word phrase" 
        2. Tokenized format: PHRASE "word1" "word2" "word3"
        
        Both convert to: PHRASE("multi word phrase")
        """
        processed = []
        i = 0
        while i < len(tokens):
            if tokens[i] == 'PHRASE':
                # PHRASE is a prefix operator - collect all following quoted terms
                phrase_terms = []
                i += 1
                
                # Collect consecutive quoted terms after PHRASE
                while i < len(tokens) and tokens[i].startswith('"'):
                    # Extract content from quotes
                    content = tokens[i].strip('"')
                    phrase_terms.append(content)
                    i += 1
                
                if len(phrase_terms) == 0:
                    print(f"Error: PHRASE operator requires at least one quoted string")
                    return []
                
                # Join all collected terms (handles both single "multi word" and multiple "word1" "word2")
                # If single term contains spaces, keep as-is
                # If multiple single-word terms, join with spaces
                full_phrase = ' '.join(phrase_terms)
                
                # Create phrase token: PHRASE("term1 term2 term3")
                phrase_query = 'PHRASE("' + full_phrase + '")'
                processed.append(phrase_query)
                # Don't increment i - we already advanced it in the while loop
            else:
                processed.append(tokens[i])
                i += 1
        
        return processed

    def query(self, query_str: str) -> List[str]:
        """
        Parses and executes a boolean query using standard Shunting-yard and RPN evaluation.
        Supports AND, OR, NOT (unary), PHRASE, parentheses, and single terms.
        Operator precedence: PHRASE > NOT > AND > OR
        
        If skip pointers are enabled, optimizes simple AND queries.
        """
        if self.inverted_index is None:
            print("Index not loaded.")
            return []
        
        # OPTIMIZATION: For simple two-term AND queries with skip pointers enabled
        if self.skip_pointers_enabled and self.skip_index:
            # Check if this is a simple "term1" AND "term2" query
            import re
            simple_and = re.match(r'^"([^"]+)"\s+AND\s+"([^"]+)"$', query_str.strip())
            if simple_and:
                term1_raw = simple_and.group(1)
                term2_raw = simple_and.group(2)
                
                # Preprocess terms
                from .preprocessor import preprocess_text
                tokens1 = preprocess_text(term1_raw)
                tokens2 = preprocess_text(term2_raw)
                
                if tokens1 and tokens2:
                    term1 = tokens1[0]
                    term2 = tokens2[0]
                    
                    # Use skip pointer intersection
                    result_docs = self.skip_index.intersect(term1, term2)
                    return sorted(result_docs)

        tokens = self._tokenize_query(query_str)
        if not tokens: return []
        
        # Preprocess PHRASE operators into special tokens
        tokens = self._preprocess_phrase_queries(tokens)

        output_queue = []
        operator_stack = []

        # --- Standard Shunting-yard Algorithm ---
        # Operator precedence: NOT (3) > AND (2) > OR (1)
        # PHRASE is preprocessed into special tokens, not treated as operator here
        precedence = {'OR': 1, 'AND': 2, 'NOT': 3}
        # associativity = {'OR': 'left', 'AND': 'left', 'NOT': 'right'}

        for token in tokens:
            if token.startswith('"') or token.startswith('PHRASE("'): # Term or Phrase
                output_queue.append(token)
            elif token == '(':
                operator_stack.append(token)
            elif token == ')':
                while operator_stack and operator_stack[-1] != '(':
                    output_queue.append(operator_stack.pop())
                if not operator_stack or operator_stack[-1] != '(':
                    print(f"Error: Mismatched parentheses in query: {query_str}")
                    return []
                operator_stack.pop() # Pop '('
            elif token in precedence: # Operator
                # Pop operators with >= precedence (for left-associativity)
                while (operator_stack and operator_stack[-1] != '(' and
                       precedence.get(operator_stack[-1], 0) >= precedence[token]):
                     output_queue.append(operator_stack.pop())
                operator_stack.append(token)
            else:
                 print(f"Error: Unknown token during shunting yard: '{token}'")
                 return []

        # Pop remaining operators
        while operator_stack:
            if operator_stack[-1] == '(':
                print(f"Error: Mismatched parentheses in query: {query_str}")
                return []
            output_queue.append(operator_stack.pop())

        # --- Standard RPN Evaluation using a Stack ---
        eval_stack = []
        all_doc_ids = set(self.documents.keys())

        for token in output_queue:
            if token.startswith('PHRASE("'):
                # Extract phrase terms from PHRASE("term1 term2 ...") format
                phrase_content = token[8:-2]  # Remove 'PHRASE("' and '")'
                phrase_terms = [f'"{term}"' for term in phrase_content.split()]
                result = self._check_phrase(phrase_terms)
                eval_stack.append(result)
            elif token.startswith('"'):
                eval_stack.append(self._get_postings(token))
            elif token == 'AND':
                if len(eval_stack) < 2:
                    print(f"Error: Invalid syntax near AND in query: {query_str}")
                    return []
                right = eval_stack.pop()
                left = eval_stack.pop()
                
                # Regular set intersection (skip pointers don't help for intermediate results)
                eval_stack.append(left.intersection(right))
                    
            elif token == 'OR':
                if len(eval_stack) < 2:
                    print(f"Error: Invalid syntax near OR in query: {query_str}")
                    return []
                right = eval_stack.pop()
                left = eval_stack.pop()
                
                # Regular set union (skip pointers don't help for intermediate results)
                eval_stack.append(left.union(right))
            elif token == 'NOT':
                # Treat NOT strictly as unary complement
                if len(eval_stack) < 1:
                    print(f"Error: Invalid syntax near NOT in query: {query_str}")
                    return []
                operand = eval_stack.pop()
                eval_stack.append(all_doc_ids.difference(operand))
            else:
                 print(f"Error: Unknown token in evaluation queue '{token}' for query: {query_str}")
                 return []

        # Final result check
        if len(eval_stack) == 1:
            return sorted(list(eval_stack[0]))
        else:
            # Check for common error: adjacent terms without operator
            # Example RPN: "A" "B" -> leaves two items on stack
            print(f"Error: Invalid query expression overall: '{query_str}'. Final stack size: {len(eval_stack)}")
            return []
    
    def query_daat(self, query_str: str) -> List[str]:
        """
        DAAT (Document-at-a-Time) query processing for Boolean queries.
        More efficient for AND operations with sorted posting lists.
        
        For simple queries, converts to DAAT-style processing.
        For complex Boolean expressions, falls back to TAAT (query()).
        """
        if self.inverted_index is None:
            print("Index not loaded.")
            return []
        
        # Parse query to detect simple vs complex
        tokens = self._tokenize_query(query_str)
        if not tokens:
            return []
        
        # Check if it's a simple AND query (can use DAAT optimization)
        is_simple_and = all(
            token.startswith('"') or token == 'AND' 
            for token in tokens
        ) and 'AND' in tokens
        
        if is_simple_and:
            # Extract terms only (remove AND operators)
            terms = [token for token in tokens if token.startswith('"')]
            
            # Get posting lists for all terms
            posting_lists = []
            for term in terms:
                postings = self._get_postings(term)
                if not postings:  # If any term has no matches, result is empty
                    return []
                posting_lists.append(sorted(list(postings)))
            
            # DAAT: Multi-way merge with AND logic
            result = self._daat_and_merge(posting_lists)
            return sorted(list(result))
        else:
            # For complex queries (OR, NOT, phrases, parentheses), use TAAT
            return self.query(query_str)
    
    def _daat_and_merge(self, sorted_posting_lists: List[List[str]]) -> Set[str]:
        """
        DAAT implementation: Merge sorted posting lists using AND logic.
        Advances pointers document-at-a-time, checking if all lists contain the doc.
        """
        if not sorted_posting_lists or not all(sorted_posting_lists):
            return set()
        
        result = set()
        num_lists = len(sorted_posting_lists)
        pointers = [0] * num_lists
        
        # Continue while all pointers are valid
        while all(pointers[i] < len(sorted_posting_lists[i]) for i in range(num_lists)):
            # Get current doc_id at each pointer
            current_docs = [sorted_posting_lists[i][pointers[i]] for i in range(num_lists)]
            
            # Check if all doc_ids are the same (AND match)
            if len(set(current_docs)) == 1:
                # All pointers point to same document - it's a match!
                result.add(current_docs[0])
                # Advance all pointers
                for i in range(num_lists):
                    pointers[i] += 1
            else:
                # Not all match - advance pointer(s) with smallest doc_id
                min_doc = min(current_docs)
                for i in range(num_lists):
                    if current_docs[i] == min_doc:
                        pointers[i] += 1
        
        return result
    
    # --- Other required placeholder methods ---
    def update_index(self, index_id: str, remove_docs: Iterable[Dict], add_docs: Iterable[Dict]): pass
    def delete_index(self, index_id: str): pass
    def list_indices(self) -> Iterable[str]: pass
    def list_indexed_files(self, index_id: str) -> Iterable[str]: pass