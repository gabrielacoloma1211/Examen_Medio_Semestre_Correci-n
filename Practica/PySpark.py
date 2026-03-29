from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Megaline").getOrCreate()

def read_table(db_table):
    return spark.read.format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", db_table) \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", jdbc_driver) \
        .load()

#leo las tablas 
fct_eventos = read_table("gold.fct_eventos")  
dim_plans   = read_table("gold.dim_plans")

#solo 2025
fct_2025 = fct_eventos.filter(F.year("date_id") == 2025)

#ingreso promedio
result = (
    fct_2025
    .groupBy("plan_name")
    .agg(
        F.avg("monthly_revenue").alias("avg_monthly_revenue"),
        F.sum("monthly_revenue").alias("total_revenue"),
        F.count("user_id").alias("active_users")
    )
    .join(dim_plans.select("plan_name", "usd_monthly_pay"), on="plan_name", how="left")
    .orderBy(F.desc("avg_monthly_revenue"))
)

result.show()
