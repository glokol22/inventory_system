import mysql.connector
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
db_host = os.getenv('db_host')
db_user = os.getenv('db_user')
db_password = os.getenv('db_password')

# Connect to MySQL
connection = mysql.connector.connect(
    host=db_host,
    user=db_user,
    password=db_password,
    database='java_house_inventory'
)

cursor = connection.cursor()





# Create Products Table
create_products_table = """
CREATE TABLE IF NOT EXISTS Products (
    product_id VARCHAR(10) PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    sku VARCHAR(50) UNIQUE,
    category_name VARCHAR(50) NOT NULL,
    unit_of_measure VARCHAR(20),
    unit_price DECIMAL(10, 2) NOT NULL,
    product_description TEXT,
    product_weight DECIMAL(10, 2),
    product_dimensions VARCHAR(50),
    product_color VARCHAR(20),
    discount_eligibility VARCHAR(3) DEFAULT 'No',
    is_active VARCHAR(3) DEFAULT 'Yes',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
"""
cursor.execute(create_products_table)
connection.commit()
print("Products table created successfully.")




# Create Branches Table
create_branches_table = """
CREATE TABLE IF NOT EXISTS Branches (
    branch_id VARCHAR(10) PRIMARY KEY,
    branch_name VARCHAR(100) NOT NULL,
    branch_location_code VARCHAR(10) UNIQUE,
    branch_city VARCHAR(50) NOT NULL,
    branch_phone VARCHAR(20),
    branch_operating_hours VARCHAR(100),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
"""
cursor.execute(create_branches_table)
connection.commit()
print("Branches table created successfully.")




# Create Suppliers Table
create_suppliers_table = """
CREATE TABLE IF NOT EXISTS Suppliers (
    supplier_id INT PRIMARY KEY AUTO_INCREMENT,
    supplier_name VARCHAR(100) NOT NULL,
    supplier_contact_person VARCHAR(100),
    supplier_phone VARCHAR(25),
    supplier_email VARCHAR(100),
    supplier_country VARCHAR(50),
    supplier_payment_terms VARCHAR(100),
    supplier_rating DECIMAL(3, 2),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
"""
cursor.execute(create_suppliers_table)
connection.commit()
print("Suppliers table created successfully.")



# Create Customers Table
create_customers_table = """
CREATE TABLE IF NOT EXISTS Customers (
    customer_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_name VARCHAR(100) NOT NULL,
    customer_phone_number VARCHAR(25),
    customer_email VARCHAR(100),
    customer_address VARCHAR(255),
    customer_loyalty_points INT DEFAULT 0,
    customer_since DATE,
    customer_last_order_date DATE,
    customer_total_orders INT DEFAULT 0,
    customer_notes TEXT,
    customer_acquisition_channel VARCHAR(50),
    customer_segment VARCHAR(50),
    customer_gender ENUM('Male', 'Female', 'Other'),
    number_of_visits INT DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
"""
cursor.execute(create_customers_table)
connection.commit()
print("Customers table created successfully.")




# Create Orders Table
create_orders_table = """
CREATE TABLE IF NOT EXISTS Orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    branch_id VARCHAR(10),
    order_date DATE NOT NULL,
    order_status ENUM('Pending', 'In Progress', 'Completed', 'Cancelled') NOT NULL,
    order_notes TEXT,
    order_shipping_cost DECIMAL(8, 2) DEFAULT 0.00,
    order_payment_method VARCHAR(50),
    order_type ENUM('Dine-in', 'Takeaway', 'Delivery') DEFAULT 'Dine-in',
    discount_amount DECIMAL(8, 2) DEFAULT 0.00,
    coupon_code VARCHAR(20),
    modes_of_payment VARCHAR(255),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id),
    FOREIGN KEY (branch_id) REFERENCES Branches(branch_id)
);
"""
cursor.execute(create_orders_table)
connection.commit()
print("Orders table created successfully.")



# Create Inventory Table
create_inventory_table = """
CREATE TABLE IF NOT EXISTS Inventory (
    inventory_id INT PRIMARY KEY AUTO_INCREMENT,
    branch_id VARCHAR(10) NOT NULL,
    product_id VARCHAR(10) NOT NULL,
    quantity_on_hand DECIMAL(10, 2) NOT NULL,
    min_stock_level DECIMAL(10, 2),
    max_stock_level DECIMAL(10, 2),
    reorder_level INT,
    stock_status ENUM('In Stock', 'Low Stock', 'Out of Stock', 'Backordered'),
    expiry_date DATE,
    batch_number VARCHAR(50),
    last_stock_update DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (branch_id) REFERENCES Branches(branch_id),
    FOREIGN KEY (product_id) REFERENCES Products(product_id),
    UNIQUE (branch_id, product_id, expiry_date, batch_number) -- To avoid duplicate inventory entries for the same batch at a branch
);
"""
cursor.execute(create_inventory_table)
connection.commit()
print("Inventory table created successfully.")



# Create Product_Suppliers Table
create_product_suppliers_table = """
CREATE TABLE IF NOT EXISTS Product_Suppliers (
    product_supplier_id INT PRIMARY KEY AUTO_INCREMENT,
    product_id VARCHAR(10) NOT NULL,
    supplier_id INT NOT NULL,
    is_active VARCHAR(3) DEFAULT 'Yes',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES Products(product_id),
    FOREIGN KEY (supplier_id) REFERENCES Suppliers(supplier_id),
    UNIQUE (product_id, supplier_id) -- Ensure a product is not listed with the same supplier multiple times
);
"""
cursor.execute(create_product_suppliers_table)
connection.commit()
print("Product_Suppliers table created successfully.")



# Create Order_Items Table
create_order_items_table = """
CREATE TABLE IF NOT EXISTS Order_Items (
    order_item_id INT PRIMARY KEY AUTO_INCREMENT,
    order_id INT NOT NULL,
    product_id VARCHAR(10) NOT NULL,
    quantity INT NOT NULL,
    unit_price_at_order DECIMAL(10, 2) NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES Orders(order_id),
    FOREIGN KEY (product_id) REFERENCES Products(product_id)
);
"""
cursor.execute(create_order_items_table)
connection.commit()
print("Order_Items table created successfully.")



print("\n--- Data Migration Part 1: Functions for Getting or Inserting Data ---")

# Fetch all data from the denormalized table
cursor.execute("SELECT * FROM denormalized_inventory")
denormalized_data = cursor.fetchall()
denormalized_columns = [col[0] for col in cursor.description]

# Dictionaries to store IDs to avoid duplicates
products_cache = {}
branches_cache = {}
suppliers_cache = {}
customers_cache = {}
orders_cache = {}



# Function to get or insert product
def get_or_insert_product(row):
    product_id_index = denormalized_columns.index('product_id')
    product_name_index = denormalized_columns.index('product_name')
    sku_index = denormalized_columns.index('sku')
    category_name_index = denormalized_columns.index('category_name')
    unit_of_measure_index = denormalized_columns.index('unit_of_measure')
    unit_price_index = denormalized_columns.index('unit_price')
    product_description_index = denormalized_columns.index('product_description')
    product_weight_index = denormalized_columns.index('product_weight')
    product_dimensions_index = denormalized_columns.index('product_dimensions')
    product_color_index = denormalized_columns.index('product_color')
    discount_eligibility_index = denormalized_columns.index('discount_eligibility')
    is_active_index = denormalized_columns.index('is_active')

    product_id = row[product_id_index]
    if product_id not in products_cache:
        product_data = (
            product_id, row[product_name_index], row[sku_index], row[category_name_index],
            row[unit_of_measure_index], row[unit_price_index], row[product_description_index],
            row[product_weight_index], row[product_dimensions_index], row[product_color_index],
            row[discount_eligibility_index], row[is_active_index]
        )
        try:
            cursor.execute("""
                INSERT INTO Products (product_id, product_name, sku, category_name, unit_of_measure,
                                      unit_price, product_description, product_weight, product_dimensions,
                                      product_color, discount_eligibility, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, product_data)
            connection.commit()
            products_cache[product_id] = True
        except mysql.connector.IntegrityError:
            # Product already exists
            products_cache[product_id] = True
    return product_id



# Function to get or insert branch
def get_or_insert_branch(row):
    branch_name_index = denormalized_columns.index('branch_name')
    branch_location_code_index = denormalized_columns.index('branch_location_code')
    branch_city_index = denormalized_columns.index('branch_city')
    branch_phone_index = denormalized_columns.index('branch_phone')
    branch_operating_hours_index = denormalized_columns.index('branch_operating_hours')
    branch_id_val = row[branch_location_code_index] # Using location code as branch_id for simplicity in migration

    if branch_id_val not in branches_cache:
        branch_data = (
            branch_id_val, row[branch_name_index], row[branch_location_code_index],
            row[branch_city_index], row[branch_phone_index], row[branch_operating_hours_index]
        )
        try:
            cursor.execute("""
                INSERT INTO Branches (branch_id, branch_name, branch_location_code, branch_city,
                                       branch_phone, branch_operating_hours)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, branch_data)
            connection.commit()
            branches_cache[branch_id_val] = True
        except mysql.connector.IntegrityError:
            # Branch already exists
            branches_cache[branch_id_val] = True
    return branch_id_val




# Function to get or insert supplier
def get_or_insert_supplier(row):
    supplier_name_index = denormalized_columns.index('supplier_name')
    supplier_contact_person_index = denormalized_columns.index('supplier_contact_person')
    supplier_phone_index = denormalized_columns.index('supplier_phone')
    supplier_email_index = denormalized_columns.index('supplier_email')
    supplier_country_index = denormalized_columns.index('supplier_country')
    supplier_payment_terms_index = denormalized_columns.index('supplier_payment_terms')
    supplier_rating_index = denormalized_columns.index('supplier_rating')
    supplier_name = row[supplier_name_index]

    if supplier_name not in suppliers_cache:
        supplier_data = (
            row[supplier_name_index], row[supplier_contact_person_index], row[supplier_phone_index],
            row[supplier_email_index], row[supplier_country_index], row[supplier_payment_terms_index],
            row[supplier_rating_index]
        )
        try:
            cursor.execute("""
                INSERT INTO Suppliers (supplier_name, supplier_contact_person, supplier_phone,
                                       supplier_email, supplier_country, supplier_payment_terms,
                                       supplier_rating)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, supplier_data)
            connection.commit()
            suppliers_cache[supplier_name] = cursor.lastrowid
        except mysql.connector.IntegrityError:
            # Supplier already exists (based on name) - Fetch existing ID
            cursor.execute("SELECT supplier_id FROM Suppliers WHERE supplier_name = %s", (supplier_name,))
            suppliers_cache[supplier_name] = cursor.fetchone()[0]
    return suppliers_cache[supplier_name]



# Function to get or insert customer
def get_or_insert_customer(row):
    customer_name_index = denormalized_columns.index('customer_name')
    customer_phone_number_index = denormalized_columns.index('customer_phone_number')
    customer_email_index = denormalized_columns.index('customer_email')
    customer_address_index = denormalized_columns.index('customer_address')
    customer_loyalty_points_index = denormalized_columns.index('customer_loyalty_points')
    customer_since_index = denormalized_columns.index('customer_since')
    customer_last_order_date_index = denormalized_columns.index('customer_last_order_date')
    customer_total_orders_index = denormalized_columns.index('customer_total_orders')
    customer_notes_index = denormalized_columns.index('customer_notes')
    customer_acquisition_channel_index = denormalized_columns.index('customer_acquisition_channel')
    customer_segment_index = denormalized_columns.index('customer_segment')
    customer_gender_index = denormalized_columns.index('customer_gender')
    number_of_visits_index = denormalized_columns.index('number_of_visits')
    customer_phone = row[customer_phone_number_index]

    if customer_phone not in customers_cache:
        customer_data = (
            row[customer_name_index], row[customer_phone_number_index], row[customer_email_index],
            row[customer_address_index], row[customer_loyalty_points_index], row[customer_since_index],
            row[customer_last_order_date_index], row[customer_total_orders_index], row[customer_notes_index],
            row[customer_acquisition_channel_index], row[customer_segment_index], row[customer_gender_index],
            row[number_of_visits_index]
        )
        try:
            cursor.execute("""
                INSERT INTO Customers (customer_name, customer_phone_number, customer_email,
                                       customer_address, customer_loyalty_points, customer_since,
                                       customer_last_order_date, customer_total_orders, customer_notes,
                                       customer_acquisition_channel, customer_segment, customer_gender,
                                       number_of_visits)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, customer_data)
            connection.commit()
            customers_cache[customer_phone] = cursor.lastrowid
        except mysql.connector.IntegrityError:
            # Customer already exists (based on phone) - Fetch existing ID
            cursor.execute("SELECT customer_id FROM Customers WHERE customer_phone_number = %s", (customer_phone,))
            customers_cache[customer_phone] = cursor.fetchone()[0]
    return customers_cache[customer_phone]



# Function to get or insert order
def get_or_insert_order(row, customer_id, branch_id):
    order_id_index = denormalized_columns.index('order_id')
    order_date_index = denormalized_columns.index('order_date')
    order_status_index = denormalized_columns.index('order_status')
    order_notes_index = denormalized_columns.index('order_notes')
    order_shipping_cost_index = denormalized_columns.index('order_shipping_cost')
    order_payment_method_index = denormalized_columns.index('order_payment_method')
    order_type_index = denormalized_columns.index('order_type')
    discount_amount_index = denormalized_columns.index('discount_amount')
    coupon_code_index = denormalized_columns.index('coupon_code')
    modes_of_payment_index = denormalized_columns.index('modes_of_payment')
    order_id_val = row[order_id_index]

    if order_id_val not in orders_cache:
        order_data = (
            order_id_val, customer_id, branch_id, row[order_date_index],
            row[order_status_index], row[order_notes_index], row[order_shipping_cost_index],
            row[order_payment_method_index], row[order_type_index], row[discount_amount_index],
            row[coupon_code_index], row[modes_of_payment_index]
        )
        try:
            cursor.execute("""
                INSERT INTO Orders (order_id, customer_id, branch_id, order_date, order_status,
                                    order_notes, order_shipping_cost, order_payment_method, order_type,
                                    discount_amount, coupon_code, modes_of_payment)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, order_data)
            connection.commit()
            orders_cache[order_id_val] = True
        except mysql.connector.IntegrityError:
            # Order already exists
            orders_cache[order_id_val] = True
    return order_id_val



print("\n--- Data Migration Part 2: Iterating and Inserting Data ---")

# Migrate data
for row in denormalized_data:
    # Insert/Get Product
    product_id = get_or_insert_product(row)

    # Insert/Get Branch
    branch_id = get_or_insert_branch(row)

    # Insert/Get Supplier and Product_Supplier link
    supplier_id = get_or_insert_supplier(row)
    try:
        cursor.execute("""
            INSERT INTO Product_Suppliers (product_id, supplier_id, is_active)
            VALUES (%s, %s, %s)
        """, (product_id, supplier_id, row[denormalized_columns.index('is_active')]))
        connection.commit()
    except mysql.connector.IntegrityError:
        # Link already exists
        pass

    # Insert/Get Customer
    customer_id = get_or_insert_customer(row)

    # Insert/Get Order
    order_id = get_or_insert_order(row, customer_id, branch_id)

    # Insert Order Item
    order_quantity_index = denormalized_columns.index('order_quantity')
    unit_price_index = denormalized_columns.index('unit_price') # Price at the time of denormalization
    try:
        cursor.execute("""
            INSERT INTO Order_Items (order_id, product_id, quantity, unit_price_at_order)
            VALUES (%s, %s, %s, %s)
        """, (order_id, product_id, row[order_quantity_index], row[unit_price_index]))
        connection.commit()
    except mysql.connector.IntegrityError:
        # Likely a duplicate order item, though less probable with this structure
        pass

    # Insert Inventory
    quantity_on_hand_index = denormalized_columns.index('quantity_on_hand')
    min_stock_level_index = denormalized_columns.index('min_stock_level')
    max_stock_level_index = denormalized_columns.index('max_stock_level')
    reorder_level_index = denormalized_columns.index('reorder_level')
    stock_status_index = denormalized_columns.index('stock_status')
    expiry_date_index = denormalized_columns.index('expiry_date')
    batch_number_index = denormalized_columns.index('batch_number')

    inventory_data = (
        branch_id, product_id, row[quantity_on_hand_index], row[min_stock_level_index],
        row[max_stock_level_index], row[reorder_level_index], row[stock_status_index],
        row[expiry_date_index], row[batch_number_index]
    )
    try:
        cursor.execute("""
            INSERT INTO Inventory (branch_id, product_id, quantity_on_hand, min_stock_level,
                                   max_stock_level, reorder_level, stock_status, expiry_date,
                                   batch_number)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, inventory_data)
        connection.commit()
    except mysql.connector.IntegrityError:
        # Inventory for this product at this branch with this batch/expiry already exists
        pass



print("\nData migration to normalized tables complete.")

# Close the connection
cursor.close()
connection.close()

print("Normalization process finished.")