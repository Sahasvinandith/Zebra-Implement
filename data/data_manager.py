import pandas as pd
import os
# # Load CSVs
# customer_df = pd.read_csv("customer_data.csv")
# product_df = pd.read_csv("products_list.csv")

# --- FILE PATHS ---
INPUT_DIR = "input"
CUSTOMER_FILE = os.path.join(INPUT_DIR, "customer_data.csv")
PRODUCT_FILE = os.path.join(INPUT_DIR, "products_list.csv")

# --- LOAD CSVs ---
customer_df = pd.read_csv(CUSTOMER_FILE)
product_df = pd.read_csv(PRODUCT_FILE)

# ----------------------------
# SEARCH FUNCTIONS
# ----------------------------
def search_customer_by_id(customer_id):
    """Search and display customer details by Customer_ID."""
    result = customer_df[customer_df["Customer_ID"] == customer_id]
    if result.empty:
        print(f"\n No customer found with Customer_ID '{customer_id}'")
        return None
    print("\n Customer details found:")
    print(result.to_string(index=False))
    return result.to_dict(orient="records")[0]


def search_product_by_id(product_id):
    """Search and display product details by SKU."""
    result = product_df[product_df["SKU"] == product_id]
    if result.empty:
        print(f"\n No product found with SKU '{product_id}'")
        return None
    print("\n Product details found:")
    print(result.to_string(index=False))
    return result.to_dict(orient="records")[0]


def search_product_field(product_id, field_name):
    """Search a specific field (column) of a product by SKU."""
    result = product_df[product_df["SKU"] == product_id]
    if result.empty:
        print(f"\n No product found with SKU '{product_id}'")
        return None
    if field_name not in product_df.columns:
        print(f"\n Column '{field_name}' not found in product data.")
        print("Available columns:", list(product_df.columns))
        return None
    value = result.iloc[0][field_name]
    print(f"\n {field_name} for product SKU '{product_id}' is: {value}")
    return value


# ----------------------------
# EDIT FUNCTIONS
# ----------------------------
def edit_customer_value(customer_id, column_name, new_value):
    """Edit a specific column value for a customer."""
    global customer_df
    if column_name not in customer_df.columns:
        print(f"\n Column '{column_name}' not found in customer data.")
        print("Available columns:", list(customer_df.columns))
        return
    index = customer_df.index[customer_df["Customer_ID"] == customer_id]
    if len(index) == 0:
        print(f"\n No customer found with Customer_ID '{customer_id}'")
        return
    customer_df.loc[index, column_name] = new_value
    customer_df.to_csv("customer_data.csv", index=False)
    print(f"\n Updated '{column_name}' for Customer_ID '{customer_id}' to '{new_value}'")


def edit_product_value(product_id, column_name, new_value):
    """Edit a specific column value for a product."""
    global product_df
    if column_name not in product_df.columns:
        print(f"\n Column '{column_name}' not found in product data.")
        print("Available columns:", list(product_df.columns))
        return
    index = product_df.index[product_df["SKU"] == product_id]
    if len(index) == 0:
        print(f"\n No product found with SKU '{product_id}'")
        return
    product_df.loc[index, column_name] = new_value
    product_df.to_csv("products_list.csv", index=False)
    print(f"\n Updated '{column_name}' for product SKU '{product_id}' to '{new_value}'")

