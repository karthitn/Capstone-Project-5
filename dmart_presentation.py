import streamlit as st
#Presentation
st.title(":blue[Dmart analysis using pyspark]")
st.header(":rainbow[Problem Statement]")
st.markdown("The task is to create a data pipeline using PySpark to integrate and analyze sales data from three different sources: product information, sales transactions, and customer details. The goal is to establish a connection with PySpark, load the datasets, perform data transformations, and answer a set of analytical questions.")    
st.subheader(":orange[Technologies Used:]")
st.markdown("Python, SQL , Pyspark")    
st.subheader(":orange[Project Workflow:]")
st.markdown("Data retrieval from the specified link, Load and clean the product, sales, and customer data.Merge the datasets and create new features for analysis.Perform exploratory data analysis to uncover insights.")   
st.subheader(":orange[Task performed to complete the project:]")
st.markdown("""
- **:green[Establish PySpark Connection]** - Set up a PySpark environment.Create a connection to read CSV files into PySpark DataFrames.
- **:green[Load Data into PySpark DataFrames]** - Load the products.csv, sales.csv, and customer.csv files into separate PySpark DataFrames.
- **:green[Data Transformation and Cleaning]** - Perform necessary data cleaning and Rename columns for consistency if needed.Ensure data types are correctly set for each column.Join the DataFrames on relevant keys (Product ID and Customer ID).
- **:green[Data Analysis and Querying]** - Perform exploratory data analysis to uncover insights.                                               
""")   
st.subheader(":orange[Task performed in Databricks Environment:]")
st.markdown("""
**:red[1.Establish PySpark Connection]** \n
:green[code:] from pyspark.sql import SparkSession \n
from pyspark.sql.types import DecimalType \n
spark = SparkSession.builder \ \n
      .master("local[1]") \ \n
      .appName("SalesDataPipeline") \ \n
      .getOrCreate()  \n                                      
**:red[2.Load Data into PySpark DataFrames]** \n
:green[code:] sales_df = spark.read.csv("/FileStore/tables/Sales.csv",header=True,inferSchema=True) \n                                              
**:red[3.Data Transformation and Cleaning - Renaming Columns]** \n
:green[code:] sales_df = sales_df.withColumnRenamed("Ship Date","Ship_Date").withColumnRenamed("Ship Mode","Ship_Mode") \n
:violet[Limit the decimal points for sales column]\n    
sales_df = sales_df.withColumn("Sales", sales_df["Sales"].cast(DecimalType(10, 2)))\n
:violet[Join the DataFrames:]\n    
joined_df = sales_df.join(product_df, "product_id").join(customer_df, "customer_id")\n                
**:red[4.Data Analysis and Querying]** \n
:green[code:] joined_df.createOrReplaceTempView("sales_view")\n
df2 = spark.sql("SELECT count(DISTINCT(product_id)) from sales_view")\n
df2.show()\n                                                                
""") 
st.subheader("", divider=True)
st.subheader("Thank you :smile: !!", divider=True)
