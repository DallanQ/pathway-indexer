import os
import time
import json
import dotenv
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
        "total_nodes_generated": 0,
        "average_nodes_per_file": 0,
        "files_with_zero_nodes": 0,
        "files_with_one_node": 0,
        "files_with_more_than_one_node": 0,
        "documents_indexed": 0,
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

        stats["total_nodes_generated"] = len(nodes)
        stats["documents_indexed"] = len(nodes)

        for node in nodes:
            filepath = node.metadata.get("filepath")
            if filepath not in stats["node_counts_per_file"]:
                stats["node_counts_per_file"][filepath] = 0
            stats["node_counts_per_file"][filepath] += 1

        for filepath, count in stats["node_counts_per_file"].items():
            if count == 0:
                stats["files_with_zero_nodes"] += 1
            elif count == 1:
                stats["files_with_one_node"] += 1
            else:
                stats["files_with_more_than_one_node"] += 1
        
        if len(stats["node_counts_per_file"]) > 0:
            stats["average_nodes_per_file"] = stats["total_nodes_generated"] / len(stats["node_counts_per_file"])

        end_time = time.time()
        execution_seconds = end_time - start_time
        hours, rem = divmod(execution_seconds, 3600)
        minutes, seconds = divmod(rem, 60)
        stats["execution_time"] = f"{int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds"

        print(json.dumps(stats, indent=4))

        return index, retriever, nodes
        
    except Exception as e:
        print(f"\n❌ Error in main process: {e}")
        raise


if __name__ == "__main__":
    index, retriever, nodes = main()
