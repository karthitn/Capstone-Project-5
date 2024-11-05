# Capstone-Project-5
<h1>Dmart analysis using pyspark<br/></h1>

**Problem Statement:** <br/> The task is to create a data pipeline using PySpark to integrate and analyze sales data from three sources: product information, sales transactions, and customer details. The goal is to establish a connection with PySpark, load the datasets, perform data transformations, and answer a set of analytical questions.<br/>

**Technologies Used:** <br/> Python, SQL , Pyspark.<br/>

**Project Workflow:** <br/>Data retrieval from the specified link, Load and clean the product, sales, and customer data. Merge the datasets and create new features for analysis. Perform exploratory data analysis to uncover insights.

**Task performed to complete the project:** <br/> **1. Establish PySpark Connection** - Input: CSV files. Set up a PySpark environment. Create a connection to read CSV files into PySpark DataFrames.<br/>
**2. Load Data into PySpark DataFrames** - Load the products.csv, sales.csv, and customer.csv files into separate PySpark DataFrames. <br/>
**3. Data Transformation and Cleaning** - Rename columns for consistency if needed. Handle missing values appropriately. Ensure data types are correctly set for each column.
Join the DataFrames on relevant keys (Product ID and Customer ID). <br/>
**4. Data Analysis and Querying** - Run Queries on the Pyspark to answer analytical questions based on the integrated dataset. <br/>
