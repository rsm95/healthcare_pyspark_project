import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import StorageLevel
import os

# Configure logging
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(filename=os.path.join(log_dir, 'claims_processing.log'), level=logging.INFO)
logger = logging.getLogger(__name__)

class ClaimsProcessing:
    def __init__(self, spark, schema, file_path, frequent_claim_threshold=2):
        self.spark = spark
        self.schema = schema
        self.file_path = file_path
        self.frequent_claim_threshold = frequent_claim_threshold
        self.df = None

    def load_data(self):
        try:
            self.df = self.spark.read.csv(self.file_path, header=True, schema=self.schema).persist(StorageLevel.MEMORY_AND_DISK)
            logger.info("Data loaded successfully")
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise

    def calculate_stats(self, disease):
        try:
            stats = self.df.filter(col("DISEASE") == disease).agg(
                mean("claim_amount_settled").alias("mean_claim"),
                stddev("claim_amount_settled").alias("stddev_claim")
            ).collect()

            if not stats:
                raise ValueError(f"No data found for disease: {disease}")

            mean_claim = stats[0]["mean_claim"]
            stddev_claim = stats[0]["stddev_claim"]

            logger.info(f"Mean and standard deviation calculated for {disease}")
            logger.info(f"Mean Claim: {mean_claim}, Standard Deviation: {stddev_claim}")

            return mean_claim, stddev_claim
        except Exception as e:
            logger.error(f"Error calculating stats for {disease}: {e}")
            raise

    def identify_outliers(self, mean_claim, stddev_claim):
        try:
            threshold_high_2sigma = mean_claim + 2 * stddev_claim
            threshold_high_3sigma = mean_claim + 3 * stddev_claim

            outliers_2sigma = self.df.filter(col("claim_amount_settled") > threshold_high_2sigma).persist(StorageLevel.MEMORY_AND_DISK)
            outliers_3sigma = self.df.filter(col("claim_amount_settled") > threshold_high_3sigma).persist(StorageLevel.MEMORY_AND_DISK)

            logger.info("Outliers identified")
            logger.info(f"Threshold High 2 Sigma: {threshold_high_2sigma}, Threshold High 3 Sigma: {threshold_high_3sigma}")

            return outliers_2sigma, outliers_3sigma
        except Exception as e:
            logger.error(f"Error identifying outliers: {e}")
            raise

    def identify_frequent_and_fraudulent_claims(self, disease, threshold_high_2sigma):
        try:
            frequent_claims = self.df.groupBy("hospital_id").count().withColumnRenamed("count", "claim_count")
            frequent_claims = frequent_claims.withColumn("frequent_claims", when(col("claim_count") > self.frequent_claim_threshold, "Yes").otherwise("No"))

            broadcast_frequent_claims = broadcast(frequent_claims)

            fraudulent_claims = self.df.withColumn("high_value_claim", when(col("claim_amount_settled") > threshold_high_2sigma, "Yes").otherwise("No"))
            fraudulent_claims = fraudulent_claims.join(broadcast_frequent_claims, on="hospital_id", how="left")
            fraudulent_claims = fraudulent_claims.filter((col("high_value_claim") == "Yes") | (col("frequent_claims") == "Yes"))
            fraudulent_claims = fraudulent_claims.filter(col("DISEASE") == disease)

            fraudulent_claims = fraudulent_claims.select("hospital_id", "claim_id", "insurance_provider_id", "claim_amount_settled", "high_value_claim", "claim_count", "frequent_claims")

            frauds = fraudulent_claims.withColumn("fraud_detected", when(((col("high_value_claim") == "Yes") & (col("frequent_claims") == "Yes")), "Yes").otherwise("No"))
            frauds = frauds.filter((col("high_value_claim") == "Yes") & (col("frequent_claims") == "Yes"))

            fraudulent_claims_count = fraudulent_claims.count()
            frauds_count = frauds.count()

            logger.info("Frequent and fraudulent claims identified")
            logger.info(f"Total Fraudulent Claims: {fraudulent_claims_count}, Total Frauds: {frauds_count}")

            return fraudulent_claims, frauds
        except Exception as e:
            logger.error(f"Error identifying frequent and fraudulent claims for {disease}: {e}")
            raise

    def process_disease(self, disease):
        try:
            mean_claim, stddev_claim = self.calculate_stats(disease)
            outliers_2sigma, outliers_3sigma = self.identify_outliers(mean_claim, stddev_claim)
            threshold_high_2sigma = mean_claim + 2 * stddev_claim
            threshold_high_3sigma = mean_claim + 3 * stddev_claim
            fraudulent_claims, frauds = self.identify_frequent_and_fraudulent_claims(disease, threshold_high_2sigma)

            logger.info(f"Processing completed for disease: {disease}")
            return {
                "mean_claim": mean_claim,
                "stddev_claim": stddev_claim,
                "threshold_high_2sigma": threshold_high_2sigma,
                "threshold_high_3sigma": threshold_high_3sigma,
                "fraudulent_claims": fraudulent_claims.show(truncate=False),
                "frauds": frauds.show(truncate=False)
            }
        except Exception as e:
            logger.error(f"Error processing disease {disease}: {e}")
            raise