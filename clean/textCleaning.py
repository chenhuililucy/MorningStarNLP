import re
from collections import Counter, defaultdict
from difflib import SequenceMatcher
import re
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import tiktoken
import logging
import concurrent.futures
import threading


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class textCleaning:
    def __init__(self, num_merges=1000, top_n=5, similarity_threshold=0.95):
        self.num_merges = num_merges
        self.top_n = top_n
        self.similarity_threshold = similarity_threshold
        self.tokenizer = tiktoken.get_encoding("cl100k_base")
        #  Load OpenAI's tokenizer (using GPT-4 encoding)
        nltk.download("punkt")
        nltk.download("stopwords")
        self.lock = threading.Lock()  # Ensure thread-safe logging
        self.processed_count = 0  # Track processed documents


    @staticmethod
    def tokenize(text):
        """Tokenizes text: lowercase, separates punctuation, normalizes spaces."""
        text = text.lower()
        text = re.sub(r'([.,!?;])', r' \1 ', text)  # Separate punctuation
        text = re.sub(r'\s+', ' ', text).strip()  # Normalize spaces
        return text.split()

    @staticmethod
    def get_word_pairs(tokens):
        """Creates adjacent word pairs from a list of tokens."""
        return list(zip(tokens, tokens[1:]))

    def merge_most_frequent(self, documents):
        """
        Iteratively merges the most frequent adjacent word pairs in documents.
        Extracts all merged phrases that have at least `top_n` words.

        Returns:
        - List[str]: Merged phrases with at least `top_n` words.
        """
        documents = [self.tokenize(text) for text in documents]

        for numMerge in range(self.num_merges):
            pair_counts = Counter()
            for tokens in documents:
                pair_counts.update(self.get_word_pairs(tokens))

            if not pair_counts:
                break

            most_frequent_pair, freq = pair_counts.most_common(1)[0]
            if freq < 2:
                break

            merged_phrase = f"{most_frequent_pair[0]}_{most_frequent_pair[1]}"

            updated_documents = []
            for tokens in documents:
                merged_tokens = []
                i = 0
                while i < len(tokens):
                    if i < len(tokens) - 1 and (tokens[i], tokens[i + 1]) == most_frequent_pair:
                        merged_tokens.append(merged_phrase)
                        i += 2
                    else:
                        merged_tokens.append(tokens[i])
                        i += 1
                updated_documents.append(merged_tokens)

            documents = updated_documents

            if  numMerge % 50 == 0:
                logging.info(f"Total merges performed: {numMerge}")
    
        # Extract only the top `top_n` longest merged phrases
        merged_phrases = set()
        for tokens in documents:
            merged_phrases.update([token for token in tokens if "_" in token])

        return [phrase for phrase in merged_phrases if len(phrase.split("_")) > self.top_n]

    @staticmethod
    def similarity_score(a, b):
        """Computes similarity score between two text segments using SequenceMatcher."""
        return SequenceMatcher(None, a, b).ratio()

    @staticmethod
    def get_first_word(text):
        """Extracts the first non-punctuation, non-numeric word from a text segment."""
        words = re.findall(r'\b[a-zA-Z]+\b', text)
        return words[0] if words else ""


    def process_documents_with_logging(self, documents, reference_phrases, num_threads=4):
        """Parallelized method for processing documents, logging progress rate and token ratio."""
        
        total_docs = len(documents)  # Total number of documents
        cleaned_documents = []
        removal_lengths = []
        token_ratios = []

        # Build dictionary where key = first word, value = list of full phrases
        phrase_dict = defaultdict(list)
        for phrase in reference_phrases:
            first_word = self.get_first_word(phrase.replace("_", " "))
            phrase_dict[first_word].append(phrase.replace("_", " "))

        def process_single_document(document):
            tokens = self.tokenize(document)
            to_remove = set()
            removed_texts = []

            for i in range(len(tokens)):
                window_text = " ".join(tokens[i:])
                window_first_word = self.get_first_word(window_text)

                # Skip if first word is not in the dictionary
                if window_first_word not in phrase_dict:
                    continue  

                # Iterate over all matching phrases for this first word
                for phrase in phrase_dict[window_first_word]:
                    phrase_length = len(phrase.split())

                    if i + phrase_length > len(tokens):
                        continue  

                    # Extract window of the same length as the phrase
                    window_text = " ".join(tokens[i:i + phrase_length])

                    # Compute similarity
                    if self.similarity_score(window_text, phrase) >= self.similarity_threshold:
                        to_remove.update(range(i, i + phrase_length))
                        removed_texts.append(window_text)  # Store removed text
                        break  

            # Compute removed text length
            total_removed_length = sum(len(text) for text in removed_texts)

            # Remove identified sections
            cleaned_tokens = [tokens[i] for i in range(len(tokens)) if i not in to_remove]
            cleaned_document = " ".join(cleaned_tokens)

            # Compute token ratio: cleaned tokens / original tokens
            original_token_count = len(tokens)
            cleaned_token_count = len(cleaned_tokens)
            token_ratio = cleaned_token_count / original_token_count if original_token_count > 0 else 0

            # Thread-safe logging & progress update
            with self.lock:
                self.processed_count += 1
                progress = (self.processed_count / total_docs) * 100
                logging.info(f"Progress: {self.processed_count}/{total_docs} ({progress:.2f}%) - Removed text length: {total_removed_length} - Token Ratio: {token_ratio:.4f}")

            return cleaned_document, total_removed_length, token_ratio

        # Use ThreadPoolExecutor for parallel processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            results = list(executor.map(process_single_document, documents))

        # Unpack results
        for cleaned_doc, removed_len, token_ratio in results:
            cleaned_documents.append(cleaned_doc)
            removal_lengths.append(removed_len)
            token_ratios.append(token_ratio)

        return cleaned_documents
        
    def process_documents(self, documents):
        """
        Full pipeline:
        1. Extracts frequent merged phrases.
        2. Removes similar sections.

        Returns:
        - List[str]: Cleaned documents.
        """
        print("\n🔍 Extracting most frequent merged phrases...")
        top_disclaimer_phrases = self.merge_most_frequent(documents[1:100])
        print(f"✅ Extracted {len(top_disclaimer_phrases)} phrases with at least {self.top_n} words.")
        print("\n🧹 Removing detected disclaimer sections...")
        cleaned_documents = self.process_documents_with_logging(documents, top_disclaimer_phrases)
        print("✅ Disclaimer sections removed.")

        return cleaned_documents

    def clean(self,text):

        text = re.sub(r'\n+', ' ', text)  # Remove new lines
        text = re.sub(r'\s+', ' ', text).strip()  # Remove extra spaces
        
        # Tokenize using OpenAI's tokenizer
        tokens = self.tokenizer.encode(text)

        # Convert tokens back into words (detokenize)
        tokenized_text = self.tokenizer.decode(tokens)

        return tokenized_text

# Example usage
# Assuming df["content_cleaned"] contains the documents
# processor = RemoveDisclaimer(num_merges=1000, top_n=5, similarity_threshold=0.95)
# cleaned_documents = processor.process_documents(df["content_cleaned"])
