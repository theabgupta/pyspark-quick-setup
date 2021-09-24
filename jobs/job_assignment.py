import sys
from pyspark.sql import Row
from pyspark.sql.functions import col, concat_ws, lit, explode, count
from dependencies.spark import start_spark


def main(args: list):
    """Main ETL script definition.

    :return: None
    """
    # start Spark application and get Spark session, logger and config
    env = args[1]
    job_name = args[2]

    # TODO : spark config read the json and add it to start_spark method

    spark, log, config = start_spark(
        app_name=f'{job_name}',
        env = env,
        files=['configs/etl_config.json'])

    # log that main ETL job is starting
    log.warn(f'{job_name} is up-and-running')

    # execute ETL pipeline
    data_map = extract_data(spark,config,log)
    data_map_transformed = transform_data(data_map,config,log)
    load_data(data_map_transformed,config,log)

    # log the success and terminate Spark application
    log.warn(f'{job_name} is finished')
    spark.stop()
    return None


def extract_data(spark,config,log) -> map:
    """Load data from Parquet file format.

    :param spark: Spark session object
    :param config: configurations
    :param log: logger object.
    :return: Spark DataFrame.
    """

    ##################################################
    ################## Extractor #####################
    ##################################################

    ####### Customer #######

    cust_df = (
        spark
            .read
            .option('inferSchema','true')
            .option('header','true')
            .csv(config['customer_path']))
    # log.debug(cust_df.show())

    ####### product #######
    prod_df = (
        spark
            .read
            .option('inferSchema', 'true')
            .option('header', 'true')
            .csv(config['product_path']))
    prod_df.printSchema()

    ####### transactions #######
    trans_df = (
        spark
            .read
            .option("multiline", "true")
            .json(config['transaction_path']))
    trans_df.printSchema()

    return {'cust_df':cust_df,'prod_df':prod_df,'trans_df':trans_df}


def transform_data(data_map, config,log) -> map:
    """Transform original dataset.

    :param data_map: extracted DataFrame map.
    :param config: configurations
        Street.
    :param log: logger object
      .
    :return: Transformed DataFrame.
    """
    cust_df = data_map['cust_df']
    prod_df = data_map['prod_df']
    trans_df = data_map['trans_df']

    # [INFO] explode the basket items, which cause increase in rows and could be end up in scewd data
    # [TODO] : repart it if required
    trans_df_stg = (
        trans_df
            .withColumn('item', explode(col("basket")).alias("basket"))
            .drop('basket')

        )

    # extract price and product from items
    trans_df_final = (
        trans_df_stg
            .withColumn('price', trans_df_stg.item.price)
            .withColumn('product_id', trans_df_stg.item.product_id)
            .drop('baskets')
            .groupby('customer_id','product_id')
            .agg(
                count(lit(1)).alias('purchase_count_per_product_id')
            )
        )
    trans_df_final.show()
    trans_df_final.printSchema()

    '''
        1. customer_id
        2. loyalty_score
        3. product_id
        4. product_category
        5. purchase_count_per_product_id
    '''
    assignment_out_df = (
        trans_df_final
            .join(cust_df, 'customer_id', 'inner')
            .join(prod_df,'product_id','inner')
            .select(
                'customer_id'
                ,'loyalty_score'
                ,'product_id'
                ,'product_category'
                ,'purchase_count_per_product_id'
            )
    )

    return {"assignment_out":assignment_out_df}


def load_data(data_map,config,log):
    """Collect data locally and write to CSV.

    :param data_map: transform DataFrame map
    :param config: configurations
    :param log: logger object
    :return: None
    """
    for key,df in data_map.items():
        (df
        .coalesce(1)
        .write
        .csv(f'{config["output"]}/{key}', mode='overwrite', header=True))

    return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    main(sys.argv)
