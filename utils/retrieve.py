import re


def generate_ngrams_from_text(text, ngram_size=3):
    """Generate ngrams from a specified text string."""

    # Lowercase and replace non-alphanumeric characters with spaces
    cleaned_text = re.sub(r"[^a-z0-9\s]", " ", text.lower())

    # Split text into words
    words = cleaned_text.split()

    # Generate ngrams
    return [tuple(words[i : i + ngram_size]) for i in range(len(words) + 1 - ngram_size)]


def generate_ngrams_from_texts(texts, ngram_size=3):
    """Generate all ngrams from a list of texts."""

    all_ngrams = []
    for text in texts:
        ngrams = generate_ngrams_from_text(text, ngram_size=ngram_size)
        all_ngrams.extend(ngrams)

    return all_ngrams


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
    return (1 + beta_squared) * (precision * recall) / (beta_squared * precision + recall)
