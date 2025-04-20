import mysql.connector
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Get credentials from the environment
db_host = os.getenv('db_host')
db_user = os.getenv('db_user')
db_password = os.getenv('db_password')

# Connect to MySQL using environment variables
connection = mysql.connector.connect(
    host=db_host,
    user=db_user,
    password=db_password,
    database='java_house_inventory'
)

cursor = connection.cursor()






# Disable foreign key checks
cursor.execute("SET foreign_key_checks = 0;")

# Drop all tables
cursor.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'java_house_inventory';
""")

tables = cursor.fetchall()
for table in tables:
    cursor.execute(f"DROP TABLE IF EXISTS {table[0]} CASCADE;")

# Enable foreign key checks again
cursor.execute("SET foreign_key_checks = 1;")

# Commit changes
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()

print("All tables and constraints have been deleted.")






from sqlalchemy import create_engine

# Load environment variables from the .env file
from dotenv import load_dotenv
import os

load_dotenv()  # Load the environment variables from the .env file

# Get credentials from the environment
db_host = os.getenv('db_host')
db_user = os.getenv('db_user')
db_password = os.getenv('db_password')

# Create the SQLAlchemy engine using the credentials
engine = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/java_house_inventory"

print("✅ SQLAlchemy engine created successfully!")




# Creating and populating denomalized inventory table   

import mysql.connector
import random
from faker import Faker
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

# Initialize Faker
fake = Faker()

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

# Revised CREATE TABLE statement with reorganized columns
create_table_query = """
CREATE TABLE IF NOT EXISTS denormalized_inventory (
    -- Product Information
    product_id VARCHAR(10) PRIMARY KEY,
    product_name VARCHAR(100),
    sku VARCHAR(50) UNIQUE,
    category_name VARCHAR(50),
    unit_of_measure VARCHAR(20),
    unit_price DECIMAL(10, 2),
    product_description TEXT,
    product_weight DECIMAL(10, 2),
    product_dimensions VARCHAR(50),
    product_color VARCHAR(20),
    expiry_date DATE,
    batch_number VARCHAR(50),

    -- Inventory Management
    quantity_on_hand DECIMAL(10, 2),
    min_stock_level DECIMAL(10, 2),
    max_stock_level DECIMAL(10, 2),
    reorder_level INT,
    stock_status ENUM('In Stock', 'Low Stock', 'Out of Stock', 'Backordered'),
    is_active VARCHAR(3) DEFAULT 'Yes',

    -- Supplier Information
    supplier_name VARCHAR(100),
    supplier_contact_person VARCHAR(100),
    supplier_phone VARCHAR(25),
    supplier_email VARCHAR(100),
    supplier_country VARCHAR(50),
    supplier_payment_terms VARCHAR(100),
    supplier_rating DECIMAL(3, 2),

    -- Order Information
    order_id INT,
    order_date DATE,
    order_status ENUM('Pending', 'In Progress', 'Completed', 'Cancelled'),
    order_notes TEXT,
    order_quantity INT,
    order_shipping_cost DECIMAL(8, 2),
    order_payment_method VARCHAR(50),
    order_type ENUM('Dine-in', 'Takeaway', 'Delivery') DEFAULT 'Dine-in',
    discount_amount DECIMAL(8, 2) DEFAULT 0.00,
    coupon_code VARCHAR(20),

    -- Customer Information
    customer_name VARCHAR(100),
    customer_phone_number VARCHAR(25),
    customer_email VARCHAR(100),
    customer_address VARCHAR(255),
    customer_loyalty_points INT DEFAULT 0,
    customer_since DATE,
    customer_last_order_date DATE,
    customer_total_orders INT,
    customer_notes TEXT,
    customer_acquisition_channel VARCHAR(50),
    customer_segment VARCHAR(50),
    customer_gender ENUM('Male', 'Female', 'Other'),
    number_of_visits INT DEFAULT 1,

    -- Branch Information
    branch_name VARCHAR(100),
    branch_location_code VARCHAR(10),
    branch_city VARCHAR(50),
    branch_phone VARCHAR(20),
    branch_operating_hours VARCHAR(100),

    -- Operational/Tracking
    discount_eligibility VARCHAR(3) DEFAULT 'No',
    modes_of_payment VARCHAR(255),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
"""
cursor.execute(create_table_query)
connection.commit()




# list of Java House branches
branches = [
    {"name": "ABC Place", "location_code": "ABC", "city": "Nairobi", "phone": "254 721 496 832", "operating_hours": "6:30 AM - 9:00 PM"},
    {"name": "Java House Adams", "location_code": "ADM", "city": "Nairobi", "phone": "254 721 555 831", "operating_hours": "6:30 AM - 9:00 PM"},
    {"name": "Java House Aga Khan Dr. Plaza", "location_code": "AGK", "city": "Nairobi", "phone": "254 741 575 120", "operating_hours": "6:30 AM - 10:00 PM"},
    {"name": "Java House Airport View", "location_code": "APV", "city": "Nairobi", "phone": "254 746 622 266", "operating_hours": "6:30 AM - 10:00 PM"},
    {"name": "Java House Airside", "location_code": "ARS", "city": "Nairobi", "phone": "254 721 371 917", "operating_hours": "24 Hours"},
    {"name": "Java House Capital", "location_code": "CAP", "city": "Nairobi", "phone": "254 701 283 292", "operating_hours": "7:00 AM - 10:00 PM"},
    {"name": "Java House Ciata Mall", "location_code": "CTA", "city": "Nairobi", "phone": "254 701 154 070", "operating_hours": "6:30 AM - 10:00 PM"},
    {"name": "Java House Centre Point, Diani Beach Rd", "location_code": "DNI", "city": "Diani", "phone": "254 741 577 313", "operating_hours": "7:00 AM - 9:00 PM"},
    {"name": "Java House Mama Ngina Street", "location_code": "MNS", "city": "Mombasa", "phone": "254 721 494 049", "operating_hours": "6:30 AM - 10:00 PM"},
    {"name": "Java House Eldoret", "location_code": "ELD", "city": "Eldoret", "phone": "254 792 322 378", "operating_hours": "7:00 AM - 10:00 PM"},
    {"name": "Java House Embassy House", "location_code": "EMB", "city": "Nairobi", "phone": "254 700 718 950", "operating_hours": "6:30 AM - 9:00 PM"},
    {"name": "Java House Parklands Road", "location_code": "PKL", "city": "Nairobi", "phone": "254 721 615 666", "operating_hours": "6:30 AM - 9:00 PM"},
    {"name": "Java House Galleria", "location_code": "GAL", "city": "Nairobi", "phone": "254 724 719 218", "operating_hours": "7:00 AM - 10:00 PM"},
    {"name": "Java House Garden City", "location_code": "GCT", "city": "Nairobi", "phone": "254 705 153 934", "operating_hours": "7:00 AM - 9:00 PM"},
    {"name": "Java House Gigiri", "location_code": "GIG", "city": "Nairobi", "phone": "254 721 425 403", "operating_hours": "6:30 AM - 9:00 PM"},
    {"name": "Java House Greenspan", "location_code": "GRP", "city": "Nairobi", "phone": "254 702 278 252", "operating_hours": "8:00 AM - 9:00 PM"},
    {"name": "Java House Hurlingham", "location_code": "HUR", "city": "Nairobi", "phone": "254 710 627 666", "operating_hours": "6:30 AM - 10:00 PM"},
    {"name": "Java House Southfield", "location_code": "STF", "city": "Nairobi", "phone": "254 796 841 537", "operating_hours": "6:30 AM - 9:00 PM"},
    {"name": "Java House Junction", "location_code": "JCT", "city": "Nairobi", "phone": "254 725 783 402", "operating_hours": "7:00 AM - 9:00 PM"},
    {"name": "Java House Karen", "location_code": "KAR", "city": "Nairobi", "phone": "254 700 718 811", "operating_hours": "7:00 AM - 9:00 PM"},
    {"name": "Java House Kenya Re", "location_code": "KRE", "city": "Nairobi", "phone": "254 719 739 776", "operating_hours": "6:30 AM - 10:00 PM"},
    {"name": "Java House Kericho", "location_code": "KCH", "city": "Kericho", "phone": "254 792 322 464", "operating_hours": "7:00 AM - 9:00 PM"},
    {"name": "Java House Kimathi", "location_code": "KMT", "city": "Nairobi", "phone": "254 725 412 164", "operating_hours": "6:30 AM - 10:00 PM"},
    {"name": "Java House Westend Mall", "location_code": "WEM", "city": "Kisumu", "phone": "254 712 652 530", "operating_hours": "7:00 AM - 10:00 PM"},
    {"name": "Java House KMA", "location_code": "KMA", "city": "Nairobi", "phone": "254 707 349 990", "operating_hours": "6:30 AM - 9:00 PM"},
    {"name": "Java House Landside", "location_code": "LND", "city": "Nairobi", "phone": "254 727 065 759", "operating_hours": "24 Hours"},
    {"name": "Java House Lavington", "location_code": "LVN", "city": "Nairobi", "phone": "254 741 575 122", "operating_hours": "6:30 AM - 10:00 PM"},
    {"name": "Java House Lenana Road", "location_code": "LNR", "city": "Nairobi", "phone": "254 741 577 312", "operating_hours": "6:30 AM - 9:00 PM"},
    {"name": "Java House Lunga Lunga", "location_code": "LGA", "city": "Nairobi", "phone": "254 705 144 000", "operating_hours": "7:00 AM - 6:00 PM"},
    {"name": "Java House Monrovia Street", "location_code": "MRO", "city": "Nairobi", "phone": "254 746 622 200", "operating_hours": "6:30 AM - 10:00 PM"}
]




# list of Java House products with their categories
product_categories = {
    "Espresso": "Coffee Beverages", "Americano": "Coffee Beverages", "Cappuccino": "Coffee Beverages", "Latte": "Coffee Beverages", "Flat White": "Coffee Beverages",
    "Macchiato": "Coffee Beverages","Mocha": "Coffee Beverages", "Iced Coffee": "Coffee Beverages", "Iced Americano": "Coffee Beverages", "Iced Latte": "Coffee Beverages",
    "Iced Mocha": "Coffee Beverages", "Cold Brew": "Coffee Beverages", "Affogato": "Coffee Beverages", "Espresso Con Panna": "Coffee Beverages",

    "Vanilla Latte": "Flavored Coffees","Caramel Macchiato": "Flavored Coffees","Hazelnut Latte": "Flavored Coffees","Pumpkin Spice Latte": "Flavored Coffees",
    "Cinnamon Dolce Latte": "Flavored Coffees","Toffee Nut Latte": "Flavored Coffees","Chocolate Hazelnut Latte": "Flavored Coffees",

    "Java House Signature Blend": "Signature Blends", "Breakfast Blend": "Signature Blends", "Espresso Roast": "Signature Blends", "French Roast": "Signature Blends",
    "Decaf Coffee": "Signature Blends", "House Blend": "Signature Blends", "Colombian Coffee": "Signature Blends", "Sumatra Coffee": "Signature Blends", "Kenyan Coffee": "Signature Blends",

    "Black Tea": "Teas", "Green Tea": "Teas", "Earl Grey Tea": "Teas", "Chai Tea Latte": "Teas", "Masala Chai": "Teas", "Herbal Tea (Chamomile)": "Teas",
    "Herbal Tea (Peppermint)": "Teas", "Iced Tea (Lemon)": "Teas", "Iced Tea (Peach)": "Teas", "Matcha Latte": "Teas", "London Fog": "Teas",

    "Hot Chocolate": "Non-Coffee Beverages", "Iced Hot Chocolate": "Non-Coffee Beverages", "Vanilla Milkshake": "Non-Coffee Beverages",
    "Caramel Milkshake": "Non-Coffee Beverages", "Strawberry Milkshake": "Non-Coffee Beverages", "Lemonade": "Non-Coffee Beverages",
    "Iced Lemonade": "Non-Coffee Beverages", "Fruit Smoothie (Mango)": "Non-Coffee Beverages", "Fruit Smoothie (Berry)": "Non-Coffee Beverages",
    "Green Smoothie": "Non-Coffee Beverages", "Protein Shake": "Non-Coffee Beverages",

    "Croissant (Plain)": "Snacks", "Croissant (Chocolate)": "Snacks", "Croissant (Almond)": "Snacks", "Muffin (Blueberry)": "Snacks",
    "Muffin (Banana)": "Snacks", "Scone (Raisin)": "Snacks", "Scone (Berry)": "Snacks", "Cinnamon Roll": "Snacks", "Brownie": "Snacks",
    "Chocolate Chip Cookie": "Snacks", "Oatmeal Cookie": "Snacks", "Fruit Tart": "Snacks", "Cheese Danish": "Snacks", "Sandwich (Chicken)": "Snacks",
    "Sandwich (Ham)": "Snacks", "Sandwich (Veggie)": "Snacks", "Wrap (Chicken Caesar)": "Snacks", "Wrap (Veggie)": "Snacks",
    "Bagel (Plain)": "Snacks", "Bagel (Sesame)": "Snacks", "Bagel (Cinnamon Raisin)": "Snacks", "Granola Bar": "Snacks", "Quiche": "Snacks",

    "Pancakes": "Breakfast", "French Toast": "Breakfast", "Eggs & Bacon": "Breakfast", "Breakfast Burrito": "Breakfast", "Oatmeal": "Breakfast",
    "Avocado Toast": "Breakfast", "Egg Sandwich": "Breakfast", "Breakfast Croissant": "Breakfast",

    "Iced Espresso": "Cold Beverages", "Iced Cold Brew": "Cold Beverages", "Iced Mocha Frappe": "Cold Beverages", "Iced Vanilla Frappe": "Cold Beverages",
    "Iced Caramel Frappe": "Cold Beverages", "Iced Green Tea Frappe": "Cold Beverages", "Iced Chai Frappe": "Cold Beverages","Iced Fruit Tea": "Cold Beverages", "Iced Matcha Latte": "Cold Beverages",

    "Pumpkin Spice Latte (Seasonal)": "Specialty Drinks", "Peppermint Mocha (Seasonal)": "Specialty Drinks", "Gingerbread Latte (Seasonal)": "Specialty Drinks",
    "Eggnog Latte (Seasonal)": "Specialty Drinks", "Holiday Blend Coffee (Seasonal)": "Specialty Drinks", "Caramel Brulee Latte (Seasonal)": "Specialty Drinks",
    "Toffee Nut Mocha (Seasonal)": "Specialty Drinks",

    "Nitro Cold Brew": "Coffee Variants", "Iced Nitro Coffee": "Coffee Variants", "Nitro Latte": "Coffee Variants", "Nitro Vanilla Latte": "Coffee Variants",
    "Coconut Milk Latte": "Coffee Variants", "Almond Milk Latte": "Coffee Variants", "Oat Milk Latte": "Coffee Variants"
}

all_products = list(product_categories.keys())





# generate a unique SKU
def generate_unique_sku(existing_skus):
    while True:
        sku = f"SKU-{random.randint(100000, 999999)}"
        if sku not in existing_skus:
            return sku



# generate random data
def generate_random_data(existing_skus):
    product_id = f"P{random.randint(100000, 999999)}"
    product_name = random.choice(all_products)
    sku = generate_unique_sku(existing_skus)
    existing_skus.add(sku)
    category_name = random.choice(list(product_categories.values()))
    unit_of_measure = random.choice(['kg', 'g', 'l', 'ml', 'piece', 'count'])
    unit_price = round(random.uniform(2, 50), 2)
    reorder_level = random.randint(5, 50)
    supplier_names = ["Farmers Best", "Global Imports", "Local Delights", "Fresh Produce Ltd", "Dairy Masters", "Grain Suppliers Inc"]
    supplier_name = random.choice(supplier_names)
    supplier_contact_person = fake.name()
    country_codes = ['+254', '+1', '+44', '+61', '+49']
    supplier_phone = f"{random.choice(country_codes)}{random.randint(1000000000, 9999999999)}"
    email_domains = ['@gmail.com', '@yahoo.com', '@outlook.com', '@company.com', '@test.io']
    name_parts = fake.name().split()
    if len(name_parts) > 1:
        supplier_email = f"{name_parts[0].lower()}.{name_parts[-1].lower()}{random.choice(email_domains)}"
    else:
        supplier_email = f"{name_parts[0].lower()}{random.choice(email_domains)}"

    order_id = random.randint(100000, 999999)
    order_date = fake.date_this_year()
    order_status = random.choice(['Pending', 'In Progress', 'Completed', 'Cancelled'])
    order_notes = fake.text(max_nb_chars=100)
    customer_name = fake.name()
    customer_phone_number = f"+254{random.randint(700000000, 799999999)}"
    customer_email = fake.name().split()[0].lower() + random.choice(email_domains)
    customer_address = fake.address()
    customer_loyalty_points = random.randint(0, 1000)
    branch = random.choice(branches)
    branch_location_code = f"BR{random.randint(100000, 999999)}"
    quantity_on_hand = random.uniform(0, 1000)
    min_stock_level = random.uniform(0, quantity_on_hand)
    max_stock_level = random.uniform(quantity_on_hand, 2000)
    today = datetime.now()
    expiry_date_start = today + timedelta(days=365)
    expiry_date_end = today + timedelta(days=730)
    expiry_date = fake.date_between_dates(date_start=expiry_date_start, date_end=expiry_date_end)
    product_description = fake.text(max_nb_chars=200)
    product_weight = round(random.uniform(0.1, 10), 2)
    product_dimensions = f"{random.randint(10, 50)}cm x {random.randint(10, 50)}cm x {random.randint(5, 20)}cm"
    product_color = random.choice(['Red', 'Blue', 'Green', 'Yellow', 'Black', 'White'])
    customer_since = fake.date_between(start_date='-5y', end_date='now')
    customer_last_order_date = fake.date_between(start_date='-1y', end_date='now')
    customer_total_orders = random.randint(1, 100)
    customer_notes = fake.text(max_nb_chars=150)
    discount_eligibility_bool = random.choice([True, False])
    discount_eligibility = "Yes" if discount_eligibility_bool else "No"
    modes_of_payment = random.choice(['Cash', 'Credit Card', 'Mobile Money', 'Voucher', 'Card,Mobile Money'])
    stock_status = random.choice(['In Stock', 'Low Stock', 'Out of Stock', 'Backordered'])
    batch_number = f"BATCH-{random.randint(100, 999)}-{fake.random_letter().upper()}"
    order_quantity = random.randint(1, 50)
    supplier_country = fake.country()
    if len(supplier_country) > 50:
        supplier_country = supplier_country[:50]
    is_active_bool = random.choice([True, False])
    is_active = "Yes" if is_active_bool else "No"
    supplier_payment_terms = random.choice(['Net 30', '2% 10 Net 30', 'Net 60', 'Cash on Delivery'])
    supplier_rating = round(random.uniform(1, 5), 2)
    customer_acquisition_channel = random.choice(['Website', 'Referral', 'In-store Promotion', 'Social Media', 'Email Marketing'])
    customer_segment = random.choice(['Retail', 'Wholesale', 'Corporate', 'VIP'])
    order_shipping_cost = round(random.uniform(5, 50), 2)
    order_payment_method = random.choice(['Visa', 'Mastercard', 'M-Pesa', 'Cash', 'Bank Transfer'])
    order_type = random.choice(['Dine-in', 'Takeaway', 'Delivery'])
    discount_amount = round(random.uniform(0, 10), 2) if random.random() < 0.3 else 0.00 # Apply discount in some cases
    coupon_code = fake.lexify(text='COUPON-????') if random.random() < 0.1 else None # Apply coupon in fewer cases
    customer_gender = random.choice(['Male', 'Female', 'Other'])
    number_of_visits = random.randint(1, 50)

    return (
        # Product Information
        product_id, product_name, sku, category_name, unit_of_measure, unit_price,
        product_description, product_weight, product_dimensions, product_color,
        expiry_date, batch_number,

        # Inventory Management
        quantity_on_hand, min_stock_level, max_stock_level, reorder_level,
        stock_status, is_active,

        # Supplier Information
        supplier_name, supplier_contact_person, supplier_phone, supplier_email,
        supplier_country, supplier_payment_terms, supplier_rating,

        # Order Information
        order_id, order_date, order_status, order_notes, order_quantity,
        order_shipping_cost, order_payment_method, order_type, discount_amount,
        coupon_code,

        # Customer Information
        customer_name, customer_phone_number, customer_email, customer_address,
        customer_loyalty_points, customer_since, customer_last_order_date,
        customer_total_orders, customer_notes, customer_acquisition_channel,
        customer_segment, customer_gender, number_of_visits,

        # Branch Information
        branch["name"], branch_location_code, branch["city"], branch["phone"],
        branch["operating_hours"],

        # Operational/Tracking
        discount_eligibility, modes_of_payment
    )




# Insert data into the database
def insert_data(num_rows=200):
    existing_skus = set()
    for _ in range(num_rows):
        data = generate_random_data(existing_skus)
        insert_query = """
        INSERT INTO denormalized_inventory (
            -- Product Information
            product_id, product_name, sku, category_name, unit_of_measure, unit_price,
            product_description, product_weight, product_dimensions, product_color,
            expiry_date, batch_number,

            -- Inventory Management
            quantity_on_hand, min_stock_level, max_stock_level, reorder_level,
            stock_status, is_active,

            -- Supplier Information
            supplier_name, supplier_contact_person, supplier_phone, supplier_email,
            supplier_country, supplier_payment_terms, supplier_rating,

            -- Order Information
            order_id, order_date, order_status, order_notes, order_quantity,
            order_shipping_cost, order_payment_method, order_type, discount_amount,
            coupon_code,

            -- Customer Information
            customer_name, customer_phone_number, customer_email, customer_address,
            customer_loyalty_points, customer_since, customer_last_order_date,
            customer_total_orders, customer_notes, customer_acquisition_channel,
            customer_segment, customer_gender, number_of_visits,

            -- Branch Information
            branch_name, branch_location_code, branch_city, branch_phone,
            branch_operating_hours,

            -- Operational/Tracking
            discount_eligibility, modes_of_payment

        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            cursor.execute(insert_query, data)
        except Exception as e:
            print(f"Error executing query: {e}")
            print(f"Insert Query: {insert_query}")
            print(f"Data: {data}")
            raise  # Re-raise the exception after printing info

    connection.commit()

# Run the insert function to add data
insert_data()

#Print message
print("Data inserted successfully!")

# Close the connection
cursor.close()
connection.close()