"""
test_job_assignment.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in job_assignment.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""

import unittest
from dependencies.spark import start_spark
from jobs.job_assignment import extract_data,transform_data,load_data


class test_job_assignment(unittest.TestCase):
    """Test suite for transformation in job_assignment
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        self.spark, self.spark_logger, self.config_dict = start_spark(app_name="job_assignment",env="test", files=['configs/etl_config.json'])
        self.test_data_path = 'test_data/outcome/output.csv'

    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def test_extract_data(self):
        data_map = extract_data(self.spark,self.config_dict,self.spark_logger)
        self.assertTrue('cust_df' in data_map.keys())
        self.assertTrue('prod_df' in data_map.keys())
        self.assertTrue('trans_df' in data_map.keys())

    def test_transform_data(self):
        """Test data transformer.

        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        # assemble
        data_map = extract_data(self.spark, self.config_dict, self.spark_logger)

        # act
        data_transformed = transform_data(data_map,self.config_dict,self.spark_logger)

        expected_data = (
            self.spark
                .read
                .option('header','true')
                .option('inferSchema', 'true')
                .csv(self.test_data_path))


        # assert
        self.assertTrue('assignment_out' in data_transformed.keys())
        self.assertTrue(expected_data.count(),data_transformed['assignment_out'].count())
        self.assertTrue([col in expected_data.columns
                         for col in data_transformed['assignment_out'].columns])

    def test_load_data(self):
        pass

if __name__ == '__main__':
    unittest.main()
