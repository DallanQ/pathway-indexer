import re
import os

import chromadb
from llama_index.core import VectorStoreIndex
from llama_index.core.ingestion import IngestionPipeline
from llama_index.core.node_parser import (
    SentenceSplitter,
    SemanticSplitterNodeParser,
    MarkdownNodeParser,
)
from llama_index.core.vector_stores.types import VectorStoreQueryMode
from llama_index.vector_stores.chroma import ChromaVectorStore
from llama_index.vector_stores.milvus import MilvusVectorStore
# from llama_index.vector_stores.qdrant import QdrantVectorStore
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.embeddings.voyageai import VoyageEmbedding
import nest_asyncio
import optuna
import pandas as pd
from pymilvus import MilvusClient
from qdrant_client import QdrantClient, AsyncQdrantClient


def _generate_ngrams_from_text(text, ngram_size=3):
    """Generate ngrams from a specified text string."""

    # Lowercase and replace non-alphanumeric characters with spaces
    cleaned_text = re.sub(r"[^a-z0-9\s]", " ", text.lower())

    # Split text into words
    words = cleaned_text.split()

    # Generate ngrams
    return [
        tuple(words[i : i + ngram_size]) for i in range(len(words) + 1 - ngram_size)
    ]


def _generate_ngrams_from_texts(texts, ngram_size=3):
    """Generate all ngrams from a list of texts."""

    all_ngrams = []
    for text in texts:
        ngrams = _generate_ngrams_from_text(text, ngram_size=ngram_size)
        all_ngrams.extend(ngrams)

    return all_ngrams

def precision_recall(predicted_ngrams, true_ngrams):
    """
    Return the precision and recall of a predicted list of ngrams by comparing
    the predicted ngrams to the true ngrams and calculating the precision and recall.
    """

    # Convert lists to sets for easier comparison
    predicted_set = set(predicted_ngrams)
    true_set = set(true_ngrams)

    # Calculate true positives, false positives, and false negatives
    true_positives = len(predicted_set & true_set)
    false_positives = len(predicted_set - true_set)
    false_negatives = len(true_set - predicted_set)

    # Calculate precision and recall
    precision = (
        true_positives / (true_positives + false_positives)
        if true_positives + false_positives > 0
        else 0
    )
    recall = (
        true_positives / (true_positives + false_negatives)
        if true_positives + false_negatives > 0
        else 0
    )

    return precision, recall



def f_score(precision, recall, beta=1.0):
    """
    Calculate the F-score (harmonic mean) of a given precision and recall,
    with an option to weight recall higher.

    Parameters:
    precision (float): Precision of the model
    recall (float): Recall of the model
    beta (float): Weight of recall in the harmonic mean (default is 1.0, which means F1 score)

    Returns:
    float: The F(beta) score
    """
    if precision + recall == 0:
        return 0.0
    beta_squared = beta**2
    return (
        (1 + beta_squared) * (precision * recall) / (beta_squared * precision + recall)
    )


def extract_question_ngrams(qa_df, ngram_size):
    """
    Extracts n-grams from a DataFrame of questions and answers.

    This function iterates over each row of the DataFrame `qa_df` and generates n-grams from the specified columns.
    The n-grams are stored in a dictionary where the keys are the questions and the values are lists of n-grams.

    Parameters:
    qa_df (pandas.DataFrame): DataFrame containing the questions and answers.
    ngram_size (int): Size of the n-grams to generate.

    Returns:
    dict: A dictionary where the keys are the questions and the values are lists of n-grams.

    Note:
    The columns 'Initials', 'Questions', 'Ideal Answer', and 'Link to Ideal Answer' are ignored during n-gram generation.
    If no n-grams are found in a row, an error message is printed and that row is skipped.
    """
    question_ngrams = {}
    for _, row in qa_df.iterrows():
        question = row["Questions"]
        all_ngrams = []
        for column in qa_df.columns:
            if column in [
                "Initials",
                "Questions",
                "Ideal Answer",
                "Link to Ideal Answer",
            ]:
                continue
            text = row[column]
            if not text:
                continue
            all_ngrams.extend(_generate_ngrams_from_text(text, ngram_size=ngram_size))
        if len(all_ngrams) == 0:
            print("ERROR: no ngrams in quotes for ", question)
            continue
        question_ngrams[question] = all_ngrams
    return question_ngrams


def objective(trial, documents, ngram_size, question_ngrams, f_beta=1.0):
    """
    This function is called by Optuna. It creates an index, run queries over the index,
    calculates the precision and recall of the results, and returns the average f-score.
    """

    #
    # Define hyperparameters
    #

    # define embedder
    embed_model_name = trial.suggest_categorical(
        "embed_model",
        [
            "text-embedding-3-small",
            "voyage-large-2-instruct",
            # GPU runs out of memory with gte model
            # 'Alibaba-NLP/gte-large-en-v1.5',  # WARNING: this downloads 1.74G of data
        ],
    )
    if embed_model_name == "text-embedding-3-small":
        embed_model = OpenAIEmbedding(
            model=embed_model_name,
            embed_batch_size=10,
            max_retries=25,
            timeout=180,
            reuse_client=False,
        )
    elif embed_model_name == "voyage-large-2-instruct":
        embed_model = VoyageEmbedding(
            model_name=embed_model_name,
        )
    elif embed_model_name == "Alibaba-NLP/gte-large-en-v1.5":
        embed_model = HuggingFaceEmbedding(
            model_name=embed_model_name, trust_remote_code=True
        )

    # define splitter
    splitter_name = trial.suggest_categorical(
        "splitter",
        [
            "sentence",
            "semantic",
            "markdown",
        ],
    )
    if splitter_name == "sentence":
        chunk_size = trial.suggest_int("chunk_size", 256, 1024)
        chunk_overlap = trial.suggest_int("chunk_overlap", 0, 200)
        splitter = SentenceSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    elif splitter_name == "semantic":
        buffer_size = trial.suggest_int("buffer_size", 1, 3)
        breakpoint_percentile_threshold = trial.suggest_int(
            "breakpoint_percentile_threshold", 60, 95
        )
        include_prev_next_rel = trial.suggest_categorical(
            "include_prev_next_rel", [True, False]
        )
        splitter = SemanticSplitterNodeParser(
            buffer_size=buffer_size,
            breakpoint_percentile_threshold=breakpoint_percentile_threshold,
            include_prev_next_rel=include_prev_next_rel,
            embed_model=embed_model,
        )
    elif splitter_name == "markdown":
        include_prev_next_rel = trial.suggest_categorical(
            "include_prev_next_rel", [True, False]
        )
        splitter = MarkdownNodeParser(
            include_prev_next_rel=include_prev_next_rel,
        )

    # add metadata
    ## nothing for now

    # define index
    query_mode = VectorStoreQueryMode.DEFAULT
    index_type = trial.suggest_categorical(
        "index",
        [
            "chromadb",
            "qdrant",
            "milvus",  # need to create a (free) account at https://cloud.zilliz.com/
            # and add MILVUS_URI=your public endpoint and MILVUS_TOKEN=your token (api key) to your .env file
        ],
    )
    if index_type == "chromadb":
        chroma_client = chromadb.EphemeralClient()
        # delete collection if it exists
        if any(coll.name == "test" for coll in chroma_client.list_collections()):
            chroma_client.delete_collection("test")
        chroma_collection = chroma_client.create_collection("test")
        vector_store = ChromaVectorStore(chroma_collection=chroma_collection)
    elif index_type == "qdrant":
        query_mode = VectorStoreQueryMode.HYBRID
        client = QdrantClient(location=":memory:")
        # delete collection if it exists
        if client.collection_exists("test"):
            client.delete_collection("test")
        # create our vector store with hybrid indexing enabled
        # batch_size controls how many nodes are encoded with sparse vectors at once
        # hybrid uses Splade v1 for sparse vectors
        vector_store = QdrantVectorStore(
            collection_name="test",
            client=client,
            enable_hybrid=True,
            batch_size=20,
        )
    elif index_type == "milvus":
        query_mode = VectorStoreQueryMode.HYBRID
        milvus_k = trial.suggest_int("milvus_k", 40, 80)
        # delete collection if it exists
        client = MilvusClient(
            uri=os.environ["MILVUS_URI"], token=os.environ["MILVUS_TOKEN"]
        )
        if client.has_collection("test"):
            client.drop_collection(collection_name="test")
        client.close()
        # hybrid uses BGE-M3 for sparse vectors
        vector_store = MilvusVectorStore(
            uri=os.environ["MILVUS_URI"],
            token=os.environ["MILVUS_TOKEN"],
            collection_name="test",
            dim=len(embed_model.get_text_embedding("foo")),
            overwrite=True,
            enable_sparse=True,
            hybrid_ranker="RRFRanker",
            hybrid_ranker_params={"k": milvus_k},
        )

    # define top_k
    top_k = trial.suggest_int("top_k", 2, 5)
    sparse_top_k = top_k * 5

    #
    # Use hyperparameters to split documents into chunks, generate embeddings, and insert into an index
    #

    # create a simple ingestion pipeline: chunk the documents and create embeddings
    pipeline = IngestionPipeline(
        transformations=[
            splitter,
            embed_model,
        ]
    )

    # create an index from the vector store
    index = VectorStoreIndex.from_vector_store(
        vector_store,
        embed_model=embed_model,
    )

    # run the pipeline to generate nodes
    nodes = pipeline.run(documents=documents)
    # print('nodes', len(nodes))

    # add the nodes to the index
    index.insert_nodes(nodes)

    # assert all nodes have been indexed
    assert len(index.as_retriever(similarity_top_k=len(nodes)).retrieve("foo")) == len(
        nodes
    )

    # create a retriever from the index
    retriever = index.as_retriever(
        vector_store_query_mode=query_mode,
        similarity_top_k=top_k,
        sparse_top_k=sparse_top_k,
    )

    #
    # Evaluate the quality of the chunks retrieved from the index for the sample questions
    #

    # issue all questions and calculate the f-score on the retrieved chunks
    f_scores = []
    for question, true_ngrams in question_ngrams.items():
        response = retriever.retrieve(question)
        # print([node.id_ for node in response])
        predicted_ngrams = _generate_ngrams_from_texts(
            [node.text for node in response], ngram_size=ngram_size
        )
        precision, recall = precision_recall(predicted_ngrams, true_ngrams)
        score = f_score(precision, recall, beta=f_beta)
        f_scores.append(score)
    avg_f_score = sum(f_scores) / len(f_scores)

    # return the average f-score
    return avg_f_score
