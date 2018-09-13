### Script_name: basic_queries_over_tx_file.sh
### Description: This process loads data from tx_core (monthly), extracts some information about basic queries and save the results to hdfs
### Created by:  Juan Zapata
### Created date: 12/01/2017
### Version: 6

#start pyspark and load avro libraries
pyspark --packages com.databricks:spark-avro_2.10:2.0.1

#start logging 
log4jLogger = sc._jvm.org.apache.log4j 
log = log4jLogger.LogManager.getLogger(__name__) 

log.warn("Loading file with tx monthly")

products = sqlContext.read.format("com.databricks.spark.avro"). \
load("/user/landing/raw/text_files/products")

log.warn("Products loaded")

productTx = sqlContext.read.format("com.databricks.spark.avro"). \
load("/user/landing/raw/text_files/tx")

log.warn("Tx loaded")

productsJoined = products.join(productTx, products.product_id==productTx.tx_product_id)
#productsJoined.show()

log.warn("Products and its TXs joined")

from pyspark.sql.functions import *

productsJoinedResult=productsJoined.groupby(to_date(from_unixtime(col("product_date")/1000)). \
alias("product_formatted_date"),'product_status'). \
agg(round(sum("tx_item_subtotal"),2). \
alias("total_amount"),countDistinct("product_id"). \
alias("total_products")). \
orderBy(desc("product_formatted_date"),asc("product_status"),desc("total_amount"),asc("total_products"))

log.warn("Reduced process completed")

productsJoinedResult.rdd.map(lambda x: ",".join(map(str, x))). \
coalesce(1). \
saveAsTextFile("/user/results/tx_monthly.csv")

log.warn("CSV file created")

quit()



