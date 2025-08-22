import json
import os
import time

import dotenv
import pandas as pd
from pinecone import Pinecone, ServerlessSpec
from llama_index.core import Document
from llama_index.core.vector_stores.types import VectorStoreQueryMode
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.vector_stores.pinecone import PineconeVectorStore

from utils.hyper_functions import AltNodeParser, extract_index_metadata, run_pipeline

# Load environment variables
dotenv.load_dotenv()


def recreate_pinecone_index():
    """
    Check if Pinecone index exists, delete it if it does, and create a new one.
    """
    # Initialize Pinecone client
    pc = Pinecone()
    
    # Get index name from environment
    index_name = os.getenv("PINECONE_INDEX_NAME")
    if not index_name:
        raise ValueError("PINECONE_INDEX_NAME environment variable is required")
    
    print(f"Checking if index '{index_name}' exists...")
    
    # Check if index exists
    try:
        existing_indexes = [index.name for index in pc.list_indexes()]
        if index_name in existing_indexes:
            print(f"Index '{index_name}' exists. Deleting...")
            pc.delete_index(index_name)
            print(f"Index '{index_name}' deleted successfully.")
        else:
            print(f"Index '{index_name}' does not exist.")
    except Exception as e:
        print(f"Error checking existing indexes: {e}")
    
    # Create new index
    print(f"Creating new index '{index_name}'...")
    try:
        pc.create_index(
            name=index_name,
            dimension=3072,  # Using text-embedding-3-large dimensions
            metric="cosine",
            spec=ServerlessSpec(
                cloud="aws",
                region="us-east-1"
            )
        )
        print(f"Index '{index_name}' created successfully.")
    except Exception as e:
        print(f"Error creating index: {e}")
        raise


def get_vector_store():
    """
    Get PineconeVectorStore instance using environment variables.
    """
    api_key = os.getenv("PINECONE_API_KEY")
    index_name = os.getenv("PINECONE_INDEX_NAME")
    environment = os.getenv("PINECONE_ENVIRONMENT")
    
    if not api_key or not index_name or not environment:
        raise ValueError(
            "Please set PINECONE_API_KEY, PINECONE_INDEX_NAME, and PINECONE_ENVIRONMENT "
            "in your environment variables or .env file"
        )
    
    store = PineconeVectorStore(
        api_key=api_key,
        index_name=index_name,
        environment=environment,
    )
    return store


def load_documents():
    """
    Load documents from the configured data paths.
    """
    datapath = os.getenv("DATA_PATH")
    if not datapath:
        raise ValueError("DATA_PATH environment variable is required")
    
    origin_paths = [f"{datapath}out/from_html/", f"{datapath}out/from_pdf/"]
    
    # Read the document names from the directories
    files_list = []
    for path in origin_paths:
        if os.path.exists(path):
            files_list.extend([path + item for item in os.listdir(path)])
        else:
            print(f"Warning: Path {path} does not exist")
    
    files_list.sort()
    print(f"Files list length: {len(files_list)}")
    
    documents = []
    for filepath in files_list:
        try:
            with open(filepath, encoding="utf-8") as file:
                document = Document(text=file.read(), metadata={"filepath": filepath})
                documents.append(document)
        except Exception as e:
            print(f"Error reading file {filepath}: {e}")
    
    # Extract metadata for each document
    documents = [extract_index_metadata(doc) for doc in documents]
    
    # Print metadata keys found
    metadata_keys = set()
    for doc in documents:
        for key in doc.metadata:
            metadata_keys.add(key)
    
    print("Metadata added to documents")
    print(f"Metadata keys found: {sorted(metadata_keys)}")
    
    return documents


def setup_embedding_model():
    """
    Setup the OpenAI embedding model.
    """
    embed_model_name = "text-embedding-3-large"
    
    embed_model = OpenAIEmbedding(
        model=embed_model_name,
        embed_batch_size=100,
        max_retries=25,
        timeout=180,
        reuse_client=True,
        dimensions=3072,
    )
    return embed_model


def setup_splitter():
    """
    Setup the document splitter with optimized parameters.
    """
    split_by = "paragraph"
    embed_prev_next_sentences = 0
    embed_prev_next_paragraphs = 1
    max_embed_length = 400
    embed_index_headers = False
    embed_md_headers = True
    include_prev_next_paragraphs = 2
    max_include_length = 700
    include_index_headers = False
    include_md_headers = True
    
    splitter = AltNodeParser().from_defaults(
        split_by=split_by,
        embed_prev_next_sentences=embed_prev_next_sentences,
        embed_prev_next_paragraphs=embed_prev_next_paragraphs,
        max_embed_length=max_embed_length,
        embed_index_headers=embed_index_headers,
        embed_md_headers=embed_md_headers,
        include_prev_next_paragraphs=include_prev_next_paragraphs,
        max_include_length=max_include_length,
        include_index_headers=include_index_headers,
        include_md_headers=include_md_headers,
    )
    return splitter


def create_retriever(index):
    """
    Create and configure the retriever from the index.
    """
    query_mode = VectorStoreQueryMode.DEFAULT
    retriever_k = 35
    sparse_k = retriever_k * 5
    
    retriever = index.as_retriever(
        vector_store_query_mode=query_mode,
        similarity_top_k=retriever_k,
        sparse_top_k=sparse_k,
    )
    return retriever


def main():
    """
    Main function to recreate the vector store and process documents.
    """
    start_time = time.time()
    stats = {
        "average_nodes_per_file": 0,
        "files_with_zero_nodes": 0,
        "files_with_one_node": 0,
        "files_with_more_than_one_node": 0,
        "node_counts_per_file": {},
    }

    try:
        print("Starting vector store recreation and document processing...")
        
        # Step 1: Recreate Pinecone index
        print("\n=== Step 1: Recreating Pinecone Index ===")
        recreate_pinecone_index()
        
        # Step 2: Setup components
        print("\n=== Step 2: Setting up components ===")
        documents = load_documents()
        embed_model = setup_embedding_model()
        splitter = setup_splitter()
        vector_store = get_vector_store()
        
        # Step 3: Run the processing pipeline
        print("\n=== Step 3: Running processing pipeline ===")
        print("Starting pipeline...")
        index, nodes = run_pipeline(documents, splitter, embed_model, vector_store, False)
        print("Pipeline finished!")
        
        # Step 4: Create retriever
        print("\n=== Step 4: Creating retriever ===")
        retriever = create_retriever(index)
        
        print(f"\n✅ Process completed successfully!")
        print(f"   - Total nodes processed: {len(nodes)}")
        print(f"   - Vector store ready for queries")

        # Track all markdown files loaded for indexing (all .md files in output dirs)
        md_files_loaded_for_indexing = set()
        datapath = os.getenv("DATA_PATH")
        for subdir in ["out/from_html/", "out/from_pdf/"]:
            dir_path = os.path.join(datapath, subdir)
            if os.path.exists(dir_path):
                for fname in os.listdir(dir_path):
                    if fname.endswith(".md"):
                        md_files_loaded_for_indexing.add(os.path.join(dir_path, fname))

        # Count nodes for each file
        for md_file in md_files_loaded_for_indexing:
            stats["node_counts_per_file"].setdefault(md_file, 0)

        # Create mapping of filepath to URL from node metadata
        filepath_to_url = {}
        for node in nodes:
            filepath = node.metadata.get("filepath")
            url = node.metadata.get("url")
            if filepath and url:
                filepath_to_url[filepath] = url

        # Always try to get full data from all_links.csv using filename for comprehensive mapping
        filepath_to_full_data = {}
        try:
            all_links_path = os.path.join(os.getenv("DATA_PATH"), "all_links.csv")
            if os.path.exists(all_links_path):
                all_links_df = pd.read_csv(all_links_path)
                for _, row in all_links_df.iterrows():
                    if "filename" in row and pd.notna(row["filename"]):
                        filename = str(row["filename"]).strip()
                        # Match by filename hash
                        for md_file in md_files_loaded_for_indexing:
                            md_filename = os.path.splitext(os.path.basename(md_file))[0]
                            if md_filename == filename:
                                filepath_to_full_data[md_file] = {
                                    "URL": row.get("URL", "N/A"),
                                    "Heading": row.get("Heading", "N/A"), 
                                    "Subheading": row.get("Subheading", "N/A"),
                                    "Title": row.get("Title", "N/A"),
                                    "Role": row.get("Role", "N/A"),
                                    "Filename": row.get("filename", "N/A")
                                }
                                # Only update filepath_to_url if not already found from nodes
                                if md_file not in filepath_to_url:
                                    filepath_to_url[md_file] = row.get("URL", "URL not found")
        except Exception as e:
            print(f"Warning: Could not load URLs from all_links.csv: {e}")

        for node in nodes:
            filepath = node.metadata.get("filepath")
            if filepath in stats["node_counts_per_file"]:
                stats["node_counts_per_file"][filepath] += 1

        # Count nodes for each file and track zero-node files for error reporting
        zero_node_files_with_full_data = []
        for _filepath, count in stats["node_counts_per_file"].items():
            if count == 0:
                stats["files_with_zero_nodes"] += 1
                full_data = filepath_to_full_data.get(_filepath)
                if full_data:
                    zero_node_files_with_full_data.append({
                        "Filepath": _filepath,
                        "URL": full_data["URL"],
                        "Heading": full_data["Heading"],
                        "Subheading": full_data["Subheading"], 
                        "Title": full_data["Title"],
                        "Role": full_data["Role"],
                        "Filename": full_data["Filename"]
                    })
                else:
                    zero_node_files_with_full_data.append({
                        "Filepath": _filepath,
                        "URL": "URL not found",
                        "Heading": "N/A",
                        "Subheading": "N/A",
                        "Title": "N/A", 
                        "Role": "N/A",
                        "Filename": "N/A"
                    })
            elif count == 1:
                stats["files_with_one_node"] += 1
            else:
                stats["files_with_more_than_one_node"] += 1

        # Save zero-node files to CSV in error folder if any exist
        if zero_node_files_with_full_data:
            data_path = os.getenv("DATA_PATH")
            error_folder = os.path.join(data_path, "error")
            os.makedirs(error_folder, exist_ok=True)
            
            zero_node_df = pd.DataFrame(zero_node_files_with_full_data)
            error_csv_path = os.path.join(error_folder, "error.csv")
            with open(error_csv_path, "a") as f:
                f.write("Non-Indexable Files Report\n")
            zero_node_df.to_csv(error_csv_path, mode="a", index=False, header=True)

        stats["md_files_loaded_for_indexing"] = len(md_files_loaded_for_indexing)
        end_time = time.time()
        execution_seconds = end_time - start_time
        hours, rem = divmod(execution_seconds, 3600)
        minutes, seconds = divmod(rem, 60)
        hours_str = f"{int(hours)} hour" if int(hours) == 1 else f"{int(hours)} hours"
        minutes_str = f"{int(minutes)} minute" if int(minutes) == 1 else f"{int(minutes)} minutes"
        seconds_str = f"{int(seconds)} second" if int(seconds) == 1 else f"{int(seconds)} seconds"
        stats["execution_time"] = f"{hours_str}, {minutes_str}, {seconds_str}"
        # Calculate average nodes per file
        if len(stats["node_counts_per_file"]) > 0:
            stats["average_nodes_per_file"] = round(
                sum(stats["node_counts_per_file"].values()) / len(stats["node_counts_per_file"]),
                2
            )
        else:
            stats["average_nodes_per_file"] = 0

        # Write node_counts_per_file to a log file
        node_counts_log_path = os.path.join(os.getenv("DATA_PATH"), "node_counts_log.json")
        with open(node_counts_log_path, "w") as f:
            json.dump(stats["node_counts_per_file"], f, indent=4)

        # Collect zero-node files and their URLs
        zero_node_files = []
        for _filepath, count in stats["node_counts_per_file"].items():
            if count == 0:
                zero_node_files.append(_filepath)

        # Append indexer metrics explanation to metrics_explanation.log
        metrics_explanation_path = os.path.join(os.getenv("DATA_PATH"), "metrics_explanation.log")
        indexer_explanation = f"""
Indexer Metrics

=> Markdown files loaded for indexing: {stats['md_files_loaded_for_indexing']}
Number of markdown files present and loaded for indexing. This includes all .md files found in the output directories, regardless of node count.

=> Files with indexable content: {len([fp for fp, count in stats['node_counts_per_file'].items() if count > 0])}
Number of markdown files that produced at least one node (indexable content) and were included in the final indexer metrics.

=> Total nodes processed: {sum(stats['node_counts_per_file'].values())}
Number of nodes (chunks of content) created and indexed from the markdown files.

=> Average nodes per file: {stats['average_nodes_per_file']}
Average number of nodes per indexed file.

=> Files with zero nodes: {len(zero_node_files)}
Number of files that had no indexable content.
"""
        # Add section for files with zero nodes (no indexable content)
        indexer_explanation += "\nFiles without indexable content (zero nodes):\n"
        indexer_explanation += f"Total: {len(zero_node_files)}\n"
        for filepath in zero_node_files:
            url = None
            for doc in documents:
                if doc.metadata.get("filepath") == filepath:
                    url = doc.metadata.get("url")
                    break
            indexer_explanation += f"    - Filepath: {filepath}, URL: {url}\n"
        indexer_explanation += "\n"
        
        # Continue with rest of metrics
        indexer_explanation += f"=> Files with one node: {stats['files_with_one_node']}\nNumber of files that produced only one node.\n\n=> Files with more than one node: {stats['files_with_more_than_one_node']}\nNumber of files that produced more than one node.\n\n=> Node counts per file saved to: node_counts_log.json\nNode counts per file are logged for analysis.\n\n=> execution_time: {stats['execution_time']}\nTime taken for the indexing process.\n"
        
        with open(metrics_explanation_path, "a") as f:
            f.write(indexer_explanation)
            
        # Print path relative to repo root, starting from DATA_PATH
        rel_path = os.path.relpath(metrics_explanation_path, start=os.getcwd())
        print(f"\nWhat do these numbers mean? See ./{rel_path}")
        
        # Delete node_counts_per_file after metrics explanation
        del stats["node_counts_per_file"]
        
        return index, retriever, nodes
        
    except Exception as e:
        print(f"\n❌ Error in main process: {e}")
        raise


if __name__ == "__main__":
    index, retriever, nodes = main()
