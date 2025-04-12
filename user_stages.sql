/* user stage :- every user will be assigend with a user stage.

To view user stage use the command list @~ .

To load data or file into the user satge use put command.*/

use database raw_layer;
use schema raw_layer.stg;
create or replace TABLE CUSTOMER_DATA (
	ID VARCHAR(4000),
	NAME VARCHAR(4000)
);


list @~;

PUT file://C:/Users/Asus/OneDrive/Desktop/vscode/SCR%20FILES/customer_data.txt @~ AUTO_COMPRESS=TRUE;


/* to copy the files file from stage to table use copy into command*/

copy INTO customer_data
FROM @~     /*will copy all the files in that stage */
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');


/*to copy by skippingb header */
COPY INTO customer_data
FROM @~
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');



COPY INTO customer_data
FROM @~/customer_data.txt.gz   /* will copy the specific file*/
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');


/* user stage propertise :



Snowflake User Stage Properties

1. AUTO_COMPRESS:
   - Description: Enables automatic compression for files uploaded to the stage.
   - Default Value: TRUE
   - How to Enable:
     PUT file://<path_to_file> @~ AUTO_COMPRESS = TRUE;

2. FILE_FORMAT:
   - Description: Specifies the file format to use when loading data from the stage (e.g., CSV, JSON, PARQUET).
   - Default Value: No default format, must specify during COPY INTO or GET operations.
   - How to Enable:
     COPY INTO target_table
     FROM @~
     FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');

3. MAX_FILE_SIZE:
   - Description: Specifies the maximum file size for individual files in the stage. Snowflake splits larger files into smaller chunks.
   - Default Value: 5 GB
   - How to Enable:
     PUT file://<path_to_file> @~ MAX_FILE_SIZE = 10000000;  -- Example with 10 MB

4. NOTIFY_MISSING:
   - Description: Notifies you if a file is missing during the file transfer process.
   - Default Value: FALSE (no notification)
   - How to Enable:
     PUT file://<path_to_file> @~ NOTIFY_MISSING = TRUE;

5. COMMENT:
   - Description: Adds an optional comment or description for the stage.
   - Default Value: No comment by default.
   - How to Enable:
     CREATE STAGE my_stage
     COMMENT = 'This is my user stage for file staging';

6. FILE_ENCODING:
   - Description: Specifies the encoding for files in the stage (e.g., UTF-8, ASCII).
   - Default Value: UTF-8
   - How to Enable:
     COPY INTO customer_data
     FROM @~
     FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"', FILE_ENCODING = 'UTF-8');

7. VALIDATE_UTF8:
   - Description: Validates if the file is UTF-8 encoded when loading it from the stage.
   - Default Value: TRUE
   - How to Enable:
     COPY INTO target_table
     FROM @~
     FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"', VALIDATE_UTF8 = TRUE);

8. STAGE_STORAGE_INTEGRATION:
   - Description: Specifies the storage integration for the stage when integrating with external cloud storage like S3 or Azure Blob Storage.
   - Default Value: No storage integration by default.
   - How to Enable:
     CREATE STAGE my_stage
     URL = 's3://my_bucket/path/'
     STORAGE_INTEGRATION = my_integration;

9. MANAGED:
   - Description: Specifies whether the stage is managed (i.e., Snowflake manages the storage) or external.
   - Default Value: Managed (internal stage by default).
   - How to Enable:
     CREATE STAGE my_external_stage
     URL = 's3://my_bucket/path/'
     STORAGE_INTEGRATION = my_integration;

10. ENABLE_AUTO_REFRESH:
    - Description: Automatically refreshes the stage when new data is loaded from an external source (only for external stages).
    - Default Value: FALSE
    - How to Enable:
      CREATE STAGE my_external_stage
      URL = 's3://my_bucket/path/'
      STORAGE_INTEGRATION = my_integration
      ENABLE_AUTO_REFRESH = TRUE;
*/




CREATE OR REPLACE TABLE department (
  dept_id      VARCHAR(20)     NOT NULL,
  dept_name    VARCHAR(100)    NOT NULL,
  location     VARCHAR(100),
  created_at   VARCHAR(30),
  PRIMARY KEY (dept_id)
);



PUT file://C:/Users/Asus/OneDrive/Desktop/vscode/SCR_FILES/department_data.csv @~ AUTO_COMPRESS=TRUE;





COPY INTO department(dept_id, dept_name)
FROM (
  SELECT 
    $1,   -- dept_id
    $2    -- dept_name
  FROM @~/department_data.csv.gz    /* THIS WAY WE CAN FETCH EACH CLOUMN FROM THE FILE AND THEN LOAD */
  
)
FILE_FORMAT = (
  TYPE = 'CSV'
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
);


REMOVE @~/department_data.csv.gz;
/* remove the file from the stage */



