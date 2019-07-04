from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

def build_context(name="Reddit Spoilers"):
    conf = SparkConf()
    conf.setAppName(name)
    sc = SparkContext(conf=conf)
    return sc


def build_session(name="Reddit-Spoilers"):
    return SparkSession.builder.appName(name).getOrCreate()

