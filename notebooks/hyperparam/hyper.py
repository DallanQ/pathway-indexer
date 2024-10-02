import os
import dotenv

from llama_index.core import Document
from llama_index.core.vector_stores.types import VectorStoreQueryMode
from llama_index.embeddings.openai import OpenAIEmbedding

from utils.hyper_functions import AltNodeParser, extract_index_metadata, get_vector_store, run_pipeline

# from llama_index.vector_stores.chroma import ChromaVectorStore
# import chromadb

dotenv.load_dotenv()

datapath = os.getenv("DATA_PATH")

origin_paths = [f"{datapath}out/from_html/", f"{datapath}out/from_pdf/"]


# Read the document names from the directories:
files_list = [path + item for path in origin_paths for item in os.listdir(path)]

files_list.sort()

print("Files list length: ", len(files_list))

documents = []

for filepath in files_list:
    with open(filepath, encoding="utf-8") as file:
        document = Document(text=file.read(), metadata={"filepath": filepath})

        # add the document to a single entry list
        documents.append(document)


documents = [extract_index_metadata(doc) for doc in documents]

metadata_keys = set()
for doc in documents:
    for key in doc.metadata:
        metadata_keys.add(key)


print("Metadata added to documents")


# **************** PROGRAM ***************

embed_model_name = "text-embedding-3-large"

embed_model = OpenAIEmbedding(
    model=embed_model_name,
    embed_batch_size=100,
    max_retries=25,
    timeout=180,
    reuse_client=True,
    dimensions=3072,
)

split_by = "paragraph"  ### trial.suggest_categorical("split_by", ["sentence", "paragraph", "both"])
embed_prev_next_sentences = 0  ### trial.suggest_int("embed_prev_next_sentences", 0, 5)
embed_prev_next_paragraphs = 1  ### trial.suggest_int("embed_prev_next_paragraphs", 1, 4)
max_embed_length = 400
# Two problems with embedding index headers:
# 1. The index titles are often the exact same as our test questions, leading to data leakage
#    which makes our numbers too good.
# 2. The index titles may refer to specific parts of the page, but we associate them with the entire page
#    which means that every chunk on the page appears equally relevant and gets our retriever confused.
# So I think we'll have to rely on embedding markdown headers
embed_index_headers = False
embed_md_headers = True  ### trial.suggest_categorical("embed_md_headers", [True])
include_prev_next_paragraphs = 2
max_include_length = 700
include_index_headers = False  ### trial.suggest_categorical("include_index_headers", [False])
include_md_headers = True  ### trial.suggest_categorical("include_md_headers", [True])
splitter_name = "alt_splitter"
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


# define index
query_mode = VectorStoreQueryMode.DEFAULT
# index_type = "chromadb"

# chroma_client = chromadb.EphemeralClient()
# delete collection if it exists
# if any(coll.name == "test" for coll in chroma_client.list_collections()):
#     chroma_client.delete_collection("test")
# chroma_collection = chroma_client.create_collection("test")
# vector_store = ChromaVectorStore(chroma_collection=chroma_collection)

vector_store = get_vector_store()

retriever_threshold = 0.0
retriever_k = 35
sparse_k = retriever_k * 5
rerank_model = "rerank-lite-1"
rerank_threshold = 0.21
rerank_k = 17

print("Starting pipeline")
index, nodes = run_pipeline(documents, splitter, embed_model, vector_store, False)

print("Pipeline finished")

# create a retriever from the index
retriever = index.as_retriever(
    vector_store_query_mode=query_mode,
    similarity_top_k=retriever_k,
    sparse_top_k=sparse_k,
)
