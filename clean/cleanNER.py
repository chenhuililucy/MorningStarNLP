import pandas as pd
import spacy
import re
import pickle
import multiprocessing
from multiprocessing import Pool, Manager, cpu_count

def init_spacy():
    """Initialize spaCy model inside each worker process"""
    global nlp
    nlp = spacy.load("en_core_web_sm")  # Load inside worker

def mask_entities(text):
    """Mask organization, person, and product names in a text."""
    doc = nlp(text)  # Uses process-local nlp instance
    masked_text = text
    for ent in doc.ents:
        if ent.label_ in ["ORG", "PERSON", "PRODUCT"]:
            masked_text = re.sub(r'\b' + re.escape(ent.text) + r'\b', f'[{ent.label_}]', masked_text)
    return masked_text

def process_chunk(chunk, progress, total_chunks, lock):
    """Process a chunk of the DataFrame while logging progress."""
    chunk["transcript"] = chunk["transcript"].apply(mask_entities)

    # Log progress safely
    with lock:
        progress.value += 1
        print(f"Processed chunk {progress.value}/{total_chunks} ({(progress.value / total_chunks) * 100:.2f}%)")

    return chunk

if __name__ == '__main__':
    # Load the dataset
    with open('/Users/lichenhui/Desktop/MorningStarNLP1/MorningStarNLP/EarningsCall/motley-fool-data.pkl', 'rb') as file:
        data = pickle.load(file)

    print(f"Loaded {len(data)} rows.")

    # Split the DataFrame into chunks for multiprocessing
    num_workers = min(cpu_count(), 8)  # Use up to 8 CPU cores
    chunk_size = len(data) // num_workers
    chunks = [data.iloc[i:i + chunk_size].copy() for i in range(0, len(data), chunk_size)]
    total_chunks = len(chunks)

    print(f"Processing {total_chunks} chunks using {num_workers} CPU cores.")

    # Use multiprocessing manager to track progress
    with Manager() as manager:
        progress = manager.Value('i', 0)  # Shared counter
        lock = manager.Lock()  # Lock to prevent race conditions

        # Use multiprocessing pool to process chunks in parallel
        with Pool(num_workers, initializer=init_spacy) as pool:
            results = pool.starmap(process_chunk, [(chunk, progress, total_chunks, lock) for chunk in chunks])

    # Combine results
    data = pd.concat(results, ignore_index=True)

    # Save the updated DataFrame
    output_file = "masked_transcripts.csv"
    data.to_csv(output_file, index=False)
    print(f"Saved masked data to {output_file}")
