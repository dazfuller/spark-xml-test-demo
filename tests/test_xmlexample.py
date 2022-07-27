import importlib.resources as resources
import unittest

from pyspark.sql import DataFrame

from tests.spark import PySparkTestCase
from demo.xmlexample import unique_records

class XmlExampleTests(PySparkTestCase):
    def test_simple_execution(self):
        input_file = self.__get_file_input("sample.xml")
        df: DataFrame = self.spark.read.format("xml").option("rowTag", "person").load(input_file)
        unique_df = unique_records(df)

        unique_df.show()

        self.assertEqual(1, unique_df.count())
        
    
    def __get_file_input(self, name: str) -> str:
        package: str = "tests.resources"

        if not resources.is_resource(package, name):
            raise FileExistsError(f"The file {name} does not exist as a resource")
        
        with resources.open_binary(package, name) as r:
            return r.name