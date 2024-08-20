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
            if column in ["Initials", "Questions", "Ideal Answer", "Link to Ideal Answer"]:
                continue
            text = row[column]
            if not text:
                continue
            all_ngrams.extend(generate_ngrams_from_text(text, ngram_size=ngram_size))
        if len(all_ngrams) == 0:
            print("ERROR: no ngrams in quotes for ", question)
            continue
        question_ngrams[question] = all_ngrams
    return question_ngrams
