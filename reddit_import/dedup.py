"""This module contains functions for deduplicating a set of reddit comments."""
from pyspark.ml.feature import RegexTokenizer, MinHashLSH, CountVectorizer

def dedup_min_hash(df, column, id_col, min_distance=0.1):
    """
    Deduplicates a dataset using MinHash on a token count basis.
    """
    df.cache()
    tokenizer = RegexTokenizer(inputCol=column, outputCol="tokens")
    tokens = tokenizer.transform(df)
    cv = CountVectorizer(inputCol="tokens", outputCol="token_ids")
    vectorizer_model = cv.fit(tokens)
    with_token_ids = vectorizer_model.transform(tokens).drop("tokens").cache()
    mh = MinHashLSH(inputCol="token_ids", outputCol="hashes", seed=1, numHashTables=10)
    dedup_model = mh.fit(with_token_ids)
    joined = dedup_model.approxSimilarityJoin(with_token_ids, with_token_ids, min_distance, distCol="dist")\
        .drop("token_ids", "hashes")\
        .filter(f"datasetA.{id_col} < datasetB.{id_col}")
    duplicate_ids = joined.rdd.flatMap(lambda row: ([row.datasetA[id_col]], [row.datasetB[id_col]])).toDF()
    return df.join(duplicate_ids, duplicate_ids._1 == df[id_col], "left")\
        .where(duplicate_ids._1.isNull())\
        .drop(duplicate_ids._1)
