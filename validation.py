import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
db_host = os.getenv('db_host')
db_user = os.getenv('db_user')
db_password = os.getenv('db_password')
db_name = 'java_house_inventory'

# Create a SQLAlchemy engine
engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}')

print("Database connection established.")



# validation for primary keys   
def validate_not_null(engine, table_name, columns):
    """Checks for NULL values in specified columns of a table."""
    for col in columns:
        query = f"SELECT COUNT(*) FROM {table_name} WHERE {col} IS NULL"
        null_count = pd.read_sql(query, engine).iloc[0, 0]
        if null_count > 0:
            print(f"Violation: {table_name}.{col} has {null_count} NULL values.")
        else:
            print(f"Validation: {table_name}.{col} has no NULL values.")

print("\n--- NOT NULL Validation for Primary Key Columns ---")
validate_not_null(engine, "Products", ["product_id"])
validate_not_null(engine, "Branches", ["branch_id"])
validate_not_null(engine, "Suppliers", ["supplier_id"])
validate_not_null(engine, "Customers", ["customer_id"])
validate_not_null(engine, "Orders", ["order_id"])
validate_not_null(engine, "Inventory", ["inventory_id"])
validate_not_null(engine, "Product_Suppliers", ["product_supplier_id"])
validate_not_null(engine, "Order_Items", ["order_item_id"])




# validation for foreign keys   
def validate_foreign_key(engine, child_table, child_fk_column, parent_table, parent_pk_column):
    """Checks for orphaned foreign key values."""
    query = f"""
        SELECT ct.{child_fk_column}
        FROM {child_table} ct
        LEFT JOIN {parent_table} pt ON ct.{child_fk_column} = pt.{parent_pk_column}
        WHERE pt.{parent_pk_column} IS NULL AND ct.{child_fk_column} IS NOT NULL
    """
    orphaned_df = pd.read_sql(query, engine)
    if not orphaned_df.empty:
        print(f"Violation: {child_table}.{child_fk_column} has orphaned values (not found in {parent_table}.{parent_pk_column}):")
        print(orphaned_df.head()) # Display a few orphaned values
    else:
        print(f"Validation: {child_table}.{child_fk_column} references {parent_table}.{parent_pk_column} correctly (no orphans).")

def validate_unique_constraint(engine, table_name, columns):
    """Checks for duplicate combinations in columns with a unique constraint."""
    group_by_cols = ', '.join(columns)
    query = f"""
        SELECT {group_by_cols}, COUNT(*)
        FROM {table_name}
        GROUP BY {group_by_cols}
        HAVING COUNT(*) > 1
    """
    duplicates_df = pd.read_sql(query, engine)
    if not duplicates_df.empty:
        print(f"Violation: {table_name} has duplicate combinations in columns ({', '.join(columns)}):")
        print(duplicates_df.head()) # Display a few duplicates
    else:
        print(f"Validation: {table_name} has unique combinations in columns ({', '.join(columns)}).")

print("\n--- Foreign Key Validation (Remaining Tables) ---")
validate_foreign_key(engine, "Product_Suppliers", "product_id", "Products", "product_id")
validate_foreign_key(engine, "Product_Suppliers", "supplier_id", "Suppliers", "supplier_id")
validate_foreign_key(engine, "Order_Items", "order_id", "Orders", "order_id")
validate_foreign_key(engine, "Order_Items", "product_id", "Products", "product_id")

print("\n--- Unique Constraint Validation ---")
validate_unique_constraint(engine, "Products", ["sku"])
validate_unique_constraint(engine, "Branches", ["branch_location_code"])
validate_unique_constraint(engine, "Product_Suppliers", ["product_id", "supplier_id"])
validate_unique_constraint(engine, "Inventory", ["branch_id", "product_id", "expiry_date", "batch_number"])