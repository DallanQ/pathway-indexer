import re


def _generate_ngrams_from_text(text, ngram_size=3):
    """
    Generate ngrams from a specified text string.

    An ngram is a sequence of n words in a row.
    For example, if ngram_size=3 and the text was "You can not play the ranger.",
    this would result in the following list of ngrams:
    (you, can, not), (can, not, play), (not, play, the), (play, the ranger).
    """

    # Lowercase and replace non-alphanumeric characters with spaces
    cleaned_text = re.sub(r"[^a-z0-9\s]", " ", text.lower())

    # Split text into words
    words = cleaned_text.split()

    # Generate ngrams
    return [
        tuple(words[i : i + ngram_size]) for i in range(len(words) + 1 - ngram_size)
    ]


def generate_ngrams_from_texts(texts, ngram_size=3):
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
