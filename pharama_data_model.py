import snowflake.connector

try:
    conn = snowflake.connector.connect(
        user='BABUV',
        password='Qwertyuiopasdfghjklzxcvbnm1234567890',
        account='PKFYNLU-HG69498',
        warehouse='COMPUTE_WH',
        database='raw_layer',
        schema='STG'
    )

    cursor = conn.cursor()

    # Create product table
    cursor.execute("""
    CREATE OR REPLACE TABLE product (
        product_id INT PRIMARY KEY,
        product_name VARCHAR(200),
        product_volume VARCHAR(200),
        zip_code VARCHAR(200)
    );
    """)

    # Create geo_zip table
    cursor.execute("""
    CREATE OR REPLACE TABLE geo_zip (
        zip_code VARCHAR(200) PRIMARY KEY,
        area_name VARCHAR(200),
        country VARCHAR(200),
        business_unit VARCHAR(200),
        sales_rep_id INT
    );
    """)

    # Create sales_rep table
    cursor.execute("""
    CREATE OR REPLACE TABLE sales_rep (
        sales_rep_id INT PRIMARY KEY,
        rep_name VARCHAR(200),
        zip_code VARCHAR(200),
        product_id INT
    );
    """)

    print("Tables created successfully!")
    
  

except snowflake.connector.errors.ProgrammingError as e:
    print(f"Error occurred: {e}")
finally:
    cursor.close()
    conn.close()
    print("Connection closed.")
# This script connects to Snowflake and creates three tables: product, geo_zip, and sales_rep.

conn = snowflake.connector.connect(
        user='BABUV',
        password='Qwertyuiopasdfghjklzxcvbnm1234567890',
        account='PKFYNLU-HG69498',
        warehouse='COMPUTE_WH',
        database='raw_layer',
        schema='STG'
    )
cursor = conn.cursor()

# Create a Named Stage ###
cursor.execute("""
CREATE OR REPLACE FILE FORMAT Adhoc_files
  TYPE = CSV
  SKIP_BLANK_LINES = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1;
    """ )

cursor.execute("""
CREATE OR REPLACE STAGE Adhoc_stage """)

cursor.close()
conn.close()
print("file format created.")



file_product = r'C:\Users\Asus\OneDrive\Desktop\vscode\SCR_FILES\*Product*.csv' # Path to your local file
file_zip = r'C:\Users\Asus\OneDrive\Desktop\vscode\SCR_FILES\*Zip*.csv' # Path to your local file
file_sales = r'C:\Users\Asus\OneDrive\Desktop\vscode\SCR_FILES\*Sales*.csv' # Path to your local file
stage_name = 'Adhoc_stage'  # Name of the stage in Snowflake

# Construct the PUT command

put_command_pro = f"PUT file://{file_product} @{stage_name}/Product AUTO_COMPRESS=TRUE;"
put_command_zip = f"PUT file://{file_zip} @{stage_name}/Zip AUTO_COMPRESS=TRUE;"
put_command_sales = f"PUT file://{file_sales} @{stage_name}/Sales AUTO_COMPRESS=TRUE;"


conn = snowflake.connector.connect(
        user='BABUV',
        password='Qwertyuiopasdfghjklzxcvbnm1234567890',
        account='PKFYNLU-HG69498',
        warehouse='COMPUTE_WH',
        database='raw_layer',
        schema='STG'
    )
cursor = conn.cursor()

# Execute the PUT command to upload the file to the named stage
try:
    cursor.execute(put_command_pro)
    cursor.execute(put_command_zip)
    cursor.execute(put_command_sales)
    print(f"File uploaded successfully to {stage_name}")
except Exception as e:
    print(f"Error occurred: {e}")

# Close the cursor and connection
finally:
    cursor.close()
    conn.close()
    print("Connection closed.")



#loading data from stage to table##

try:
    conn = snowflake.connector.connect(
        user='BABUV',
        password='Qwertyuiopasdfghjklzxcvbnm1234567890',
        account='PKFYNLU-HG69498',
        warehouse='COMPUTE_WH',
        database='raw_layer',
        schema='STG'
    )

    cursor = conn.cursor()

    # copy into product table 
    cursor.execute("""
                   
    copy into RAW_LAYER.STG.PRODUCT
    from (select * from @RAW_LAYER.STG.ADHOC_STAGE/Product)
    FILE_FORMAT = (FORMAT_NAME = RAW_LAYER.STG.ADHOC_FILES);
    """)

    # Create geo_zip table
    cursor.execute("""    
     copy into RAW_LAYER.STG.GEO_ZIP
     from (select * from @RAW_LAYER.STG.ADHOC_STAGE/Zip)
     FILE_FORMAT = (FORMAT_NAME = RAW_LAYER.STG.ADHOC_FILES);
   
    """)

    # Create sales_rep table
    cursor.execute("""
    copy into RAW_LAYER.STG.SALES_REP
    from (select ltrim(rtrim($1)),ltrim(rtrim($2)),ltrim(rtrim($3)),ltrim(rtrim($4)) from @RAW_LAYER.STG.ADHOC_STAGE/Sales)
    FILE_FORMAT = (FORMAT_NAME = RAW_LAYER.STG.ADHOC_FILES);
    """)

    print("Data Loaded successfully!")
    
  

except snowflake.connector.errors.ProgrammingError as e:
    print(f"Error occurred: {e}")
finally:
    cursor.close()
    conn.close()



conn = snowflake.connector.connect(
    user='BABUV',
    password='Qwertyuiopasdfghjklzxcvbnm1234567890',
    account='PKFYNLU-HG69498',
    warehouse='COMPUTE_WH',
    database='raw_layer',
    schema='STG'
)

cursor = conn.cursor()

# Execute your query
cursor.execute("SELECT * FROM SALES_REP")
# Fetch all results
row = cursor.fetchall()
# Print each row
if not row:
    print("table is empty")
else:
    print("table has data")

cursor.close()
conn.close()