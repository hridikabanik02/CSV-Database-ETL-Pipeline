import pandas as pd
import numpy as np
import mysql.connector

pd.set_option('display.max_rows', None)

#Part 1: Loading the datasets
customers = pd.read_csv('Datasets - customers.csv')
orders = pd.read_csv('Datasets - orders.csv')
products = pd.read_csv('Datasets - products.csv')

#Displaying the datasets
#print(customers)
#print(orders);
#print(products)

#Part 2

#2.1 replace empty values in email column with null
#customers["email"]= customers["email"].replace(" ",np.nan)
customers["email"].replace(' ',None,inplace=True)

#2.1 replace the nulls in quantity column with 1
orders["quantitiy"] = orders["quantitiy"].fillna(1)
#print(orders);

#2.1 standerdize email by converting all emails to lowercase
customers["email"] = customers["email"].str.lower()
#print(customers)

#2.2 removing trailing whitespaces
customers["first_name"]=customers["first_name"].str.strip()
customers["last_name"]=customers["last_name"].str.strip()
products["product_name"]=products["product_name"].str.strip()
print(customers)
#print(products)

#2.3 adding new full name column
customers.insert(3, "full_name", customers["first_name"] + ' ' + customers["last_name"])
#print(customers)

#2.3 adding a discounted price column and calculating discounted prices for electronics
products["discounted_price"] = products["price"]
products.loc[products["category"] == "Electronics", "discounted_price"] *= 0.9

"""for index, row in products.iterrows():
    if (row["category"] == 'Electronics'):
        discount = 0.1
        products.at[index, "discounted_price"]= row["price"] * (1-discount)
    else:
        products.at[index, "discounted_price"] = row["price"]"""


print(products)
print(orders)
#2.4 creating new dataset for Customer Orders
customer_orders = pd.merge(customers, orders, on = "customer_id", how="inner")
print(customer_orders)

#2.4 creating new dataset for customer order summary
customer_order_summary = customer_orders.groupby("customer_id").agg(
    total_orders = pd.NamedAgg(column= 'order_id', aggfunc= 'count'),
    total_quantity = pd.NamedAgg(column = 'quantitiy', aggfunc = 'sum')).reset_index()
print(customer_order_summary)

#2.4 creating new dataset for orders and products
products_ordered = pd.merge(products,orders, on = "product_id", how = "inner")
products_ordered["total_cost"] = None
for i, row in products_ordered.iterrows():
    if (row["category"] == 'Electronics'):
        products_ordered.at[i, "total_cost"] = row["discounted_price"] * row["quantitiy"]
    else:
        products_ordered.at[i, "total_cost"] = row["price"] * row["quantitiy"]
print(products_ordered)

#2.4 creating new dataset for product revenue
product_revenue_summary = products_ordered.groupby("category").agg(
    total_revenue = pd.NamedAgg (column = 'total_cost', aggfunc = 'sum')

).reset_index()
print(product_revenue_summary)


print('Done')


#PART 3
# credentials for connecting to database
host = '127.0.0.1'
user = 'hridi'
password = 'Banik123,'
database = 'customerdatabase'

# Establishing MySQL connection
conn = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)
if conn.is_connected():
        print("Connection to MySQL was successful!")
# Create a cursor object to execute SQL commands
cursor = conn.cursor()

# Drop existing tables to start fresh each time the script is run
cursor.execute("DROP TABLE IF EXISTS orders;")
cursor.execute("DROP TABLE IF EXISTS customers;")
cursor.execute("DROP TABLE IF EXISTS products;")
cursor.execute("DROP TABLE IF EXISTS customer_orders;")
cursor.execute("DROP TABLE IF EXISTS customer_order_summary;")
cursor.execute("DROP TABLE IF EXISTS product_revenue_summary;")
conn.commit()

# Create the necessary tables in the database using SQL commands
create_customers_table = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    full_name VARCHAR(100),
    email VARCHAR(100),
    registration_date DATE
);
"""
create_orders_table = """
CREATE TABLE IF NOT EXISTS orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    quantitiy INT,
    product_id INT
);
"""
create_products_table = """
CREATE TABLE IF NOT EXISTS products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price FLOAT,
    discounted_price FLOAT
);
"""

create_customer_orders_table = """
CREATE TABLE IF NOT EXISTS customer_orders (
    customer_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    full_name VARCHAR(100),
    email VARCHAR(100),
    registration_date DATE,
    order_id INT,
    order_date DATE,
    quantitiy FLOAT,
    product_id INT
);
"""

create_customer_order_summary_table = """
CREATE TABLE IF NOT EXISTS customer_order_summary (
    customer_id INT PRIMARY KEY,
    total_orders INT,
    total_quantity FLOAT
);
"""

create_product_revenue_summary_table = """
CREATE TABLE IF NOT EXISTS product_revenue_summary (
    category VARCHAR(50) PRIMARY KEY,
    total_revenue FLOAT
);
"""


# Execute the SQL commands to create tables
cursor.execute(create_customers_table)
cursor.execute(create_orders_table)
cursor.execute(create_products_table)
cursor.execute(create_customer_orders_table)
cursor.execute(create_customer_order_summary_table)
cursor.execute(create_product_revenue_summary_table)

#Commit the changes
conn.commit()

# Function to insert a DataFrame into MySQL table
def insert_dataframe_to_mysql(df, table_name):
    cols = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))
    sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
    
    for index, row in df.iterrows():
        cursor.execute(sql, tuple(row))
    conn.commit()

# Load DataFrames into MySQL tables
insert_dataframe_to_mysql(customers, 'customers')
insert_dataframe_to_mysql(orders, 'orders')
insert_dataframe_to_mysql(products, 'products')
insert_dataframe_to_mysql(customer_orders, 'customer_orders')
insert_dataframe_to_mysql(customer_order_summary, 'customer_order_summary')
insert_dataframe_to_mysql(product_revenue_summary, 'product_revenue_summary')

print("Data successfully loaded into MySQL database.")

# Close the cursor and connection
cursor.close()
conn.close()
