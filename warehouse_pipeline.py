from src.common.error.error_etl import handle_etl_error
from src.common.helper.validation import validation_data
from src.config.logging_conf import logger
from src.warehouse.extract.extract_db import extract_source_db
from src.warehouse.load.load_db import load_dwh_db
from src.warehouse.transform.transform_customers import transform_customers
from src.warehouse.transform.transform_employees import transform_employees
from src.warehouse.transform.transform_inventory_tracking import transform_inventory_tracking
from src.warehouse.transform.transform_orders import transform_orders
from src.warehouse.transform.transform_products import transform_products
from src.warehouse.transform.transform_store_branch import transform_store_branch
from src.warehouse.validation.data_validation import is_valid_email, is_valid_phone, is_valid_number

if __name__ == '__main__':
    logger.info("===== Warehouse Pipeline Started =====")

    logger.info("Extracting Source, Transform and Load of Data Customers")
    customer_df = extract_source_db("public", "customers")
    transform_customer_df = transform_customers(customer_df, "customers")
    valid_data, invalid_data = validation_data(data=transform_customer_df, table_name="customers",
                                               validation_functions={"email": is_valid_email, "phone": is_valid_phone})
    logger.info(f"Total data valid: {len(valid_data)}, total data invalid: {len(invalid_data)}")
    if not invalid_data.empty:
        handle_etl_error(data=invalid_data, bucket_name='error-paccafe', table_name="dim_customers",
                         process='dwh_validation')
    load_dwh_db(valid_data, "public", "dim_customers", "nk_customer_id", "staging")
    logger.info("Done Extracting Source, Transform and Load Data Customers")

    logger.info("Extracting Source, Transform and Load of Data Employees")
    employee_df = extract_source_db("public", "employees")
    transform_employees_df = transform_employees(employee_df, "employees")
    valid_data, invalid_data = validation_data(data=transform_employees_df, table_name="employees",
                                               validation_functions={"email": is_valid_email})
    logger.info(f"Total data valid: {len(valid_data)}, total data invalid: {len(invalid_data)}")
    if not invalid_data.empty:
        handle_etl_error(data=invalid_data, bucket_name='error-paccafe', table_name="dim_customers",
                         process='dwh_validation')
    load_dwh_db(valid_data, "public", "dim_employees", "nk_employee_id", "staging")
    logger.info("Done Extracting Source, Transform and Load of Data Customers")

    logger.info("Extracting Source, Transform and Load of Data Store Branch")
    store_branch_df = extract_source_db("public", "store_branch")
    transform_store_branch_df = transform_store_branch(store_branch_df, "store_branch")
    load_dwh_db(transform_store_branch_df, "public", "dim_store_branch", "nk_store_id", "staging")
    logger.info("Done Extracting Source, Transform and Load of Store Branch")

    logger.info("Extracting Source, Transform and Load of Data Products")
    product_df = extract_source_db("public", "products")
    transform_products_df = transform_products(product_df, "products")
    valid_data, invalid_data = validation_data(data=transform_products_df, table_name="products",
                                               validation_functions={"unit_price": is_valid_number,
                                                                     'cost_price': is_valid_number})
    logger.info(f"Total data valid: {len(valid_data)}, total data invalid: {len(invalid_data)}")
    if not invalid_data.empty:
        handle_etl_error(data=invalid_data, bucket_name='error-paccafe', table_name="dim_products",
                         process='dwh_validation')
    load_dwh_db(valid_data, "public", "dim_products", "nk_product_id", "staging")
    logger.info("Done Extracting Source, Transform and Load of Data Products")

    logger.info("Extracting Source, Transform and Load of Data Fact Inventory")
    inventory_tracking_df = extract_source_db("public", "inventory_tracking")
    transform_inventory_tracking_df = transform_inventory_tracking(inventory_tracking_df, "inventory_tracking")
    valid_data, invalid_data = validation_data(data=transform_inventory_tracking_df, table_name="inventory_tracking",
                                               validation_functions={"quantity_change": is_valid_number})
    logger.info(f"Total data valid: {len(valid_data)}, total data invalid: {len(invalid_data)}")
    if not invalid_data.empty:
        handle_etl_error(data=invalid_data, bucket_name='error-paccafe', table_name="fct_inventory",
                         process='dwh_validation')
    load_dwh_db(valid_data, "public", "fct_inventory", "nk_tracking_id", "staging")
    logger.info("Done Extracting Source, Transform and Load of Data Products")

    logger.info("Extracting Source, Transform and Load of Data Fact Order")
    order_df = extract_source_db("public", "orders")
    transform_orders_df = transform_orders(order_df, "orders")
    valid_data, invalid_data = validation_data(data=transform_orders_df, table_name="orders",
                                               validation_functions={"total_amount": is_valid_number,
                                                                     "unit_price": is_valid_number,
                                                                     "quantity": is_valid_number,
                                                                     "subtotal": is_valid_number})
    logger.info(f"Total data valid: {len(valid_data)}, total data invalid: {len(invalid_data)}")
    if not invalid_data.empty:
        handle_etl_error(data=invalid_data, bucket_name='error-paccafe', table_name="fct_order",
                         process='dwh_validation')
    load_dwh_db(valid_data, "public", "fct_order", "nk_order_id", "staging")
    logger.info("Done Extracting Source, Transform and Load of Data Fact Order")
