"""This module contains functions for deduplicating a set of reddit comments."""
from pyspark.ml.feature import RegexTokenizer, MinHashLSH, CountVectorizer
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


def dedup_min_hash(df, column, id_col, min_distance=0.1):
    """
    Deduplicates a dataset using MinHash on a token count basis.

    Removes all items with a distance smaller than min_distance.
    """
    @udf("long")
    def num_nonzeros(v):
        return v.numNonzeros()
    df.cache()
    tokenizer = RegexTokenizer(inputCol=column, outputCol="tokens")
    tokens = tokenizer.transform(df)
    cv = CountVectorizer(inputCol="tokens", outputCol="token_ids")
    vectorizer_model = cv.fit(tokens)
    with_token_ids = vectorizer_model.transform(tokens).drop("tokens", column)
    with_token_ids = with_token_ids.where(num_nonzeros(with_token_ids.token_ids) > 0).cache()
    mh = MinHashLSH(inputCol="token_ids", outputCol="hashes", seed=1, numHashTables=10)
    dedup_model = mh.fit(with_token_ids)
    joined = dedup_model.approxSimilarityJoin(with_token_ids, with_token_ids, 1 - min_distance, distCol="dist")\
        .drop("token_ids", "hashes")\
        .filter(f"datasetA.{id_col} < datasetB.{id_col}")
    duplicate_ids = joined.rdd.flatMap(lambda row: (row.datasetA[id_col], row.datasetB[id_col]))\
        .distinct()\
        .map(lambda el: [el])\
        .toDF()
    return df.join(duplicate_ids, duplicate_ids._1 == df[id_col], "left")\
        .where(duplicate_ids._1.isNotNull())\
        .drop(duplicate_ids._1)
