# Inventory System Project

## Project Overview

This **Inventory System** is designed to efficiently manage and track inventory, orders, products, and customers for any business. Built using **MySQL** for data storage and **Python** for data manipulation and automation, the system enables seamless operations across multiple branches. By leveraging **SQL** queries, it analyzes customer behavior, sales trends, product performance, and more to improve decision-making and business strategies.

### Purpose and Goal
The project aims to provide a comprehensive solution for managing various business operations, from handling products and orders to analyzing sales and customer data. It helps businesses improve operational efficiency by giving them the tools to make informed decisions based on real-time data and insights.

## Key Features

- **Product Management**: Efficient tracking of product information like name, description, weight, dimensions, price, and stock quantity.
- **Order Management**: Managing customer orders with complete details including product quantity, order date, price, and product category.
- **Customer Tracking**: Storing customer data including order history, total spending, and purchasing trends.
- **Branch Sales Performance**: Analysis of sales and product performance across multiple branches.
- **Sales Analytics**: Use of complex SQL queries to generate reports on customer behavior, product sales trends, and more.
- **SQL-Based Insights**: Powerful reporting to track customer spending, order frequency, best-selling products, and top-performing branches.

## Technologies Used

- **MySQL**: A relational database used for storing data related to products, orders, customers, and more.
- **Python**: Programming language used to interact with the database and manipulate data.
- **SQL**: Structured Query Language used to query the database and generate reports.
- **Pandas**: Python library for handling and analyzing data, making it easy to work with the results of SQL queries.
- **Git**: For version control and collaboration.

## Project Structure

The project is organized in a simple, easy-to-navigate directory structure to ensure clarity:


### Files Breakdown

- **`schema.sql`**: Contains SQL code to create the necessary database schema and tables.
- **`data_build_up.py`**: Python script that generates and inserts sample data for products, orders, and customers.
- **`inventory_system.py`**: Core Python script to handle operations such as updating inventory and querying data.
- **`sql_queries.py`**: A collection of useful SQL queries to extract insights such as top-selling products, total customer spending, and sales trends.
- **`requirements.txt`**: A list of Python dependencies needed for the project.
- **`.gitignore`**: Ensures sensitive files like `.env` and temporary files are not tracked by Git.

## Getting Started

### Prerequisites

- **MySQL**: The database system used for data storage. You can install it from [MySQL Official Website](https://dev.mysql.com/downloads/).
- **Python**: Used for interacting with the database and performing operations. Install from [Python.org](https://www.python.org/).

### Clone the Repository

To get started with the project, first clone the repository to your local machine:

```bash
git clone https://github.com/glokol22/inventory_system.git
cd inventory_system