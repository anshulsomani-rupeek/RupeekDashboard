import os
import redshift_connector
from dotenv import load_dotenv

load_dotenv()

conn = redshift_connector.connect(
    host=os.getenv('REDSHIFT_HOST'),
    database=os.getenv('REDSHIFT_DB'),
    user=os.getenv('REDSHIFT_USER'),
    password=os.getenv('REDSHIFT_PASSWORD'),
    port=int(os.getenv('REDSHIFT_PORT', 5439))
)

cursor = conn.cursor()
try:
    cursor.execute("SELECT gl_no,sub_name,gl_loan_date,gl_loan_amount,gl_interest_rate,scheme_code,tenture,ltype,co_lender FROM dw.lms_t_gold_reg LIMIT 1")
    print("SUCCESS WITH TENTURE!")
    print(cursor.fetchall())
except Exception as e:
    print(f"ERROR: {e}")
finally:
    conn.close()
