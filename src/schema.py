import dataclasses
from collections import OrderedDict
from pyspark.sql import Row
from pyspark.sql.types import StructType, StringType, IntegerType, BooleanType, DateType
from datetime import datetime


def get_pyspark_type(type):
    return {
        str: StringType,
        int: IntegerType,
        bool: BooleanType,
        datetime: DateType,
    }[type]


class SchemaMixin:
    """Mixin for dataclasses to allow returning a Spark SQL schema and row."""

    @classmethod
    def schema(cls):
        elements = OrderedDict()
        for field in dataclasses.fields(cls):
            elements[field.name] = get_pyspark_type(field.type)
        return elements

    def to_row(self):
        return Row(**dataclasses.asdict(self))
