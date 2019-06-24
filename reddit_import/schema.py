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
    """Mixin for to allow returning a Spark SQL row."""

    @classmethod
    def from_row(cls, row):
        return cls(**row.asDict())

    def to_row(self):
        return {field.name: self.__getattribute__(field.name) for field in self.schema}
