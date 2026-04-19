import pandas as pd
import os

DATA_DIR = os.path.dirname(os.path.dirname(__file__))

orders_path = os.path.join(DATA_DIR, "data/raw/olist_orders_dataset.csv")
customers_path = os.path.join(DATA_DIR, "data/raw/olist_customers_dataset.csv")

orders = pd.read_csv(orders_path)
customers = pd.read_csv(customers_path)


print("Orders Data Loaded:", orders.shape)
orders.head()

print("\n Customers Data Loaded:", customers.shape)
customers.head()
