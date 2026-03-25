import os
import redshift_connector
from flask import Flask, render_template, request, jsonify

# Load variables from .env file if it exists
if os.path.exists('.env'):
    with open('.env') as f:
        for line in f:
            if line.strip() and not line.startswith('#') and '=' in line:
                key, val = line.strip().split('=', 1)
                os.environ[key.strip()] = val.strip().strip("'\"")

app = Flask(__name__)

def get_redshift_connection():
    return redshift_connector.connect(
        host=os.environ.get('REDSHIFT_HOST', ''),
        database=os.environ.get('REDSHIFT_DB', ''),
        port=int(os.environ.get('REDSHIFT_PORT', 5439)),
        user=os.environ.get('REDSHIFT_USER', ''),
        password=os.environ.get('REDSHIFT_PASSWORD', '')
    )

#approutes
@app.route('/')
def index():
    host = os.environ.get('REDSHIFT_HOST', 'Unknown Host')
    return render_template('index.html', host=host)

@app.route('/api/query', methods=['POST'])
def query():
    data = request.json
    gl_input = data.get('gl_numbers', '')
    
    # Parse and clean GL numbers
    gl_numbers = [g.strip() for g in gl_input.split(',') if g.strip()]
    if not gl_numbers:
        return jsonify({'error': 'No GL numbers provided'}), 400

    conn = None
    try:
        conn = get_redshift_connection()
        cursor = conn.cursor()

        placeholders = ', '.join(['%s'] * len(gl_numbers))
        
        query_sql = f"""
        SELECT
            right(narration,13) as gl_no,
            accdt.ac_name,
            db.*
        FROM dw.lms_daybook1 db
        LEFT JOIN dw.lms_acdet1 accdt
            ON db.fk_accode = accdt.pk_ac_code
           AND db.subcode = accdt.subcode
        WHERE RIGHT(db.narration, 13) in ({placeholders})
        ORDER BY right(narration, 13), db.acdate
        """
        
        cursor.execute(query_sql, gl_numbers)
        result = cursor.fetchall()
        
        columns = [desc[0] for desc in cursor.description]
        
        # Convert datetime objects or non-serializable objects to string
        serializable_results = []
        for row in result:
            s_row = [str(x) if x is not None else None for x in row]
            serializable_results.append(s_row)
            
        return jsonify({
            'columns': columns,
            'results': serializable_results
        })

    except Exception as e:
        error_msg = str(e)
        if "nodename nor servname provided" in error_msg:
            error_msg = ("DNS Resolution Failed. Are you disconnected from your VPN? "
                         "Please connect to the Rupeek VPN to access dw-redshift-prod.rupeek.com.")
        return jsonify({'error': error_msg}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/mapping', methods=['POST'])
def query_mapping():
    data = request.json
    field = data.get('field', 'gl')
    value = data.get('value', '').strip()
    
    # Validate field to prevent SQL injection
    valid_fields = ['id', 'losid', 'gl', 'referencenumber', 'requesterid']
    if field not in valid_fields:
        return jsonify({'error': 'Invalid field selected'}), 400
        
    if not value:
        return jsonify({'error': 'No value provided'}), 400

    conn = None
    try:
        conn = get_redshift_connection()
        cursor = conn.cursor()

        query_sql = f"SELECT * FROM temp.mapping WHERE {field} = %s"
        cursor.execute(query_sql, (value,))
        result = cursor.fetchall()
        
        columns = [desc[0] for desc in cursor.description]
        
        serializable_results = []
        for row in result:
            s_row = [str(x) if x is not None else None for x in row]
            serializable_results.append(s_row)
            
        return jsonify({
            'columns': columns,
            'results': serializable_results
        })

    except Exception as e:
        error_msg = str(e)
        if "nodename nor servname provided" in error_msg:
            error_msg = ("DNS Resolution Failed. Are you disconnected from your VPN? "
                         "Please connect to the Rupeek VPN to access dw-redshift-prod.rupeek.com.")
        return jsonify({'error': error_msg}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/crv', methods=['POST'])
def query_crv():
    data = request.json
    lms_id = data.get('lms_id', '').strip()
    
    if not lms_id:
        return jsonify({'error': 'No LMS ID provided'}), 400

    conn = None
    try:
        conn = get_redshift_connection()
        cursor = conn.cursor()

        query_sql = """
        WITH lender_map AS (
            SELECT '62be94b2beb2db743c17bc4c' AS lender_id, 'Axis' AS lender_name UNION ALL
            SELECT '5a43cfbebc40a39a3bc1207d', 'Federal Bank Limited' UNION ALL
            SELECT '5d75924e5bf87df5130767ae', 'ICICI Bank' UNION ALL
            SELECT '635a0b977762f40fc068fe03', 'Indian Bank' UNION ALL
            SELECT '5cdd3ebad54a11e613c3a6b5', 'Karur Vysya Bank' UNION ALL
            SELECT '5f916dab9617a826bd84a9f3', 'Kisetsu Saison Finance India Private Limited' UNION ALL
            SELECT '637de2591a0aab715655885b', 'NDX P2P Private Limited' UNION ALL
            SELECT '5ac38436aa1a12f3691dbf99', 'Rupeek Capital Private Limited' UNION ALL
            SELECT '60b7f0e0cbe7401f71ffcbda', 'South Indian Bank' UNION ALL
            SELECT '57288d5c3e2291476b2a0a4e', 'Yogakshemam Loans Limited' UNION ALL
            SELECT '63e340d634c9599729eb97d7', 'Cholamandalam Investment and Finance Company Limited'
        )
        SELECT
            a.lms_id,
            a.event_type,
            lm.lender_name,
            a.principal_amount,
            a.principal_repayment,
            a.repayment_type,
            a.interest_rate,
            a.interest_accrued,
            a.interest_debited,
            a.interest_repayment,
            a.slab_rate,
            a.unposted_interest,
            a.transit_cashback,
            a.transit_paid_amount,
            a.value_recorded_date::date,
            a.closing_amount,
            CAST(a.outstanding_amount AS DECIMAL(18,2)) AS outstanding_amount
        FROM dw.account_svc_customer_rupeek_view a
        LEFT JOIN lender_map lm
            ON a.lender_id = lm.lender_id
        WHERE a.lms_id = %s
          AND a.status = 'VALID'
        ORDER BY a.value_recorded_date
        """
        cursor.execute(query_sql, (lms_id,))
        result = cursor.fetchall()
        
        columns = [desc[0] for desc in cursor.description]
        
        serializable_results = []
        for row in result:
            s_row = [str(x) if x is not None else None for x in row]
            serializable_results.append(s_row)
            
        return jsonify({
            'columns': columns,
            'results': serializable_results
        })

    except Exception as e:
        error_msg = str(e)
        if "nodename nor servname provided" in error_msg:
            error_msg = ("DNS Resolution Failed. Are you disconnected from your VPN? "
                         "Please connect to the Rupeek VPN to access dw-redshift-prod.rupeek.com.")
        return jsonify({'error': error_msg}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/loan', methods=['POST'])
def query_loan():
    data = request.json
    account_type = data.get('account_type')
    account_no = data.get('account_no', '').strip()
    
    if not account_no:
        return jsonify({'error': 'No account number provided'}), 400

    where_clause = ""
    if account_type == 'lender':
        where_clause = "WHERE l1.lms_id = %s"
    elif account_type == 'rcpl':
        where_clause = "WHERE l1.support_lms_id = %s"
    else:
        return jsonify({'error': 'Invalid account type selected'}), 400

    conn = None
    try:
        conn = get_redshift_connection()
        cursor = conn.cursor()

        query_sql = f"""
        WITH lender_map AS (
            SELECT '62be94b2beb2db743c17bc4c' AS core_id, 'Axis' AS lender_name, 'axis' AS slug, 'AXIS' AS gold_benchmark UNION ALL
            SELECT '5a43cfbebc40a39a3bc1207d', 'Federal Bank Limited', 'federal', 'IBJA' UNION ALL
            SELECT '5d75924e5bf87df5130767ae', 'ICICI Bank', 'icici-bank', 'ICICI' UNION ALL
            SELECT '635a0b977762f40fc068fe03', 'Indian Bank', 'indianbank', 'INDIANBANK' UNION ALL
            SELECT '5cdd3ebad54a11e613c3a6b5', 'Karur Vysya Bank', 'kvb', 'KVB' UNION ALL
            SELECT '5f916dab9617a826bd84a9f3', 'Kisetsu Saison Finance India Private Limited', 'saison', 'AGLOC' UNION ALL
            SELECT '637de2591a0aab715655885b', 'NDX P2P Private Limited', 'liquiloans', 'AGLOC' UNION ALL
            SELECT '5ac38436aa1a12f3691dbf99', 'Rupeek Capital Private Limited', 'rupeek', 'AGLOC' UNION ALL
            SELECT '60b7f0e0cbe7401f71ffcbda', 'South Indian Bank', 'sib', 'IBJA' UNION ALL
            SELECT '57288d5c3e2291476b2a0a4e', 'Yogakshemam Loans Limited', 'yog', 'AGLOC' UNION ALL
            SELECT '63e340d634c9599729eb97d7', 'Cholamandalam Investment and Finance Company Limited', 'cholamandalam', 'AGLOC'
        )
        SELECT 
            (COALESCE(l1.principal_amount,0) + COALESCE(l2.principal_amount,0)) AS total_loan_amount,
            l1.lms_id AS lender_loan_account,
            lm1.lender_name,
            l1.status,
            l1.type,
            l1.principal_amount,
            l2.lms_id AS rcpl_loan_account,
            lm2.lender_name AS rcpl_lender_name,
            l2.principal_amount AS rcpl_principal_amount,
            l1.los_id,
            l1.tenure
        FROM dw.account_svc_loan l1
        LEFT JOIN dw.account_svc_loan l2
            ON l1.support_lms_id = l2.lms_id
        LEFT JOIN lender_map lm1
            ON l1.lender_id = lm1.core_id
        LEFT JOIN lender_map lm2
            ON l2.lender_id = lm2.core_id
        {where_clause}
        """
        cursor.execute(query_sql, (account_no,))
        result = cursor.fetchall()
        
        columns = [desc[0] for desc in cursor.description]
        
        serializable_results = []
        for row in result:
            s_row = [str(x) if x is not None else None for x in row]
            serializable_results.append(s_row)
            
        return jsonify({
            'columns': columns,
            'results': serializable_results
        })

    except Exception as e:
        error_msg = str(e)
        if "nodename nor servname provided" in error_msg:
            error_msg = ("DNS Resolution Failed. Are you disconnected from your VPN? "
                         "Please connect to the Rupeek VPN to access dw-redshift-prod.rupeek.com.")
        return jsonify({'error': error_msg}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/gold_reg', methods=['POST'])
def query_gold_reg():
    data = request.json
    gl_no = data.get('gl_no', '').strip()
    
    if not gl_no:
        return jsonify({'error': 'No GL number provided'}), 400

    conn = None
    try:
        conn = get_redshift_connection()
        cursor = conn.cursor()

        secured_query = """
        SELECT lploanid,id, loanid,loan_amount,  masterschemeid 
        FROM dw.core_loanrequest_loan
        WHERE id IN (SELECT DISTINCT id FROM temp.mapping WHERE gl = %s)
        """
        cursor.execute(secured_query, (gl_no,))
        secured_result = cursor.fetchall()
        secured_columns = [desc[0] for desc in cursor.description] if cursor.description else []
        serializable_secured = [[str(x) if x is not None else None for x in row] for row in secured_result]

        unsecured_query = """
        SELECT lploanid,id,uloanid,  sloanid, loanamount, masterschemeid 
        FROM dw.core_loanrequest_uloan
        WHERE id IN (SELECT DISTINCT id FROM temp.mapping WHERE gl = %s)
        """
        cursor.execute(unsecured_query, (gl_no,))
        unsecured_result = cursor.fetchall()
        unsecured_columns = [desc[0] for desc in cursor.description] if cursor.description else []
        serializable_unsecured = [[str(x) if x is not None else None for x in row] for row in unsecured_result]

        return jsonify({
            'secured_columns': secured_columns,
            'secured_results': serializable_secured,
            'unsecured_columns': unsecured_columns,
            'unsecured_results': serializable_unsecured
        })

    except Exception as e:
        error_msg = str(e)
        if "nodename nor servname provided" in error_msg:
            error_msg = ("DNS Resolution Failed. Are you disconnected from your VPN? "
                         "Please connect to the Rupeek VPN to access dw-redshift-prod.rupeek.com.")
        return jsonify({'error': error_msg}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/charges', methods=['POST'])
def query_charges():
    data = request.json
    lms_id = data.get('lms_id', '').strip()
    
    if not lms_id:
        return jsonify({'error': 'No LMS ID provided'}), 400

    conn = None
    try:
        conn = get_redshift_connection()
        cursor = conn.cursor()

        query_sql = """
        SELECT 
            lms_id,
            type,
            CAST(ROUND(SUM(value + tax), 2) AS DECIMAL(18,2)) AS total_charges,
            value_recorded_date::date
        FROM dw.account_svc_charges
        WHERE lms_id = %s
        GROUP BY lms_id, type, value_recorded_date
        """
        cursor.execute(query_sql, (lms_id,))
        result = cursor.fetchall()
        
        columns = [desc[0] for desc in cursor.description]
        
        serializable_results = []
        for row in result:
            s_row = [str(x) if x is not None else None for x in row]
            serializable_results.append(s_row)
            
        return jsonify({
            'columns': columns,
            'results': serializable_results
        })

    except Exception as e:
        error_msg = str(e)
        if "nodename nor servname provided" in error_msg:
            error_msg = ("DNS Resolution Failed. Are you disconnected from your VPN? "
                         "Please connect to the Rupeek VPN to access dw-redshift-prod.rupeek.com.")
        return jsonify({'error': error_msg}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/repayment', methods=['POST'])
def query_repayment():
    data = request.json
    lms_id = data.get('lms_id', '').strip()
    
    if not lms_id:
        return jsonify({'error': 'No LMS ID provided'}), 400

    conn = None
    try:
        conn = get_redshift_connection()
        cursor = conn.cursor()

        query_sql = """
        SELECT 
            lms_id,
            paid_amount,
            cashback,
            repayment_status,
            repayment_type,
            repayment_date::date
        FROM dw.account_svc_repayment
        WHERE lms_id = %s
        order by repayment_date asc
        """
        cursor.execute(query_sql, (lms_id,))
        result = cursor.fetchall()
        
        columns = [desc[0] for desc in cursor.description]
        
        serializable_results = []
        for row in result:
            s_row = [str(x) if x is not None else None for x in row]
            serializable_results.append(s_row)
            
        return jsonify({
            'columns': columns,
            'results': serializable_results
        })

    except Exception as e:
        error_msg = str(e)
        if "nodename nor servname provided" in error_msg:
            error_msg = ("DNS Resolution Failed. Are you disconnected from your VPN? "
                         "Please connect to the Rupeek VPN to access dw-redshift-prod.rupeek.com.")
        return jsonify({'error': error_msg}), 500
    finally:
        if conn:
            conn.close()

@app.route('/api/customer', methods=['POST'])
def query_customer():
    data = request.json
    gl = data.get('gl', '').strip()
    
    if not gl:
        return jsonify({'error': 'No GL/LMS ID provided'}), 400

    gl_list = [g.strip().strip("'").strip('"') for g in gl.split(',') if g.strip()]
    if not gl_list:
        return jsonify({'error': 'No GL/LMS ID provided'}), 400

    conn = None
    try:
        conn = get_redshift_connection()
        cursor = conn.cursor()

        format_strings = ','.join(['%s'] * len(gl_list))
        query_sql = f"""
        SELECT 
            firstname || ' ' || lastname AS full_name,
            phone_decrypted
        FROM dw.core_user
        WHERE id IN (
            SELECT DISTINCT requesterid  
            FROM temp.mapping 
            WHERE gl IN ({format_strings})
        )
        """
        cursor.execute(query_sql, tuple(gl_list))
        result = cursor.fetchall()
        
        columns = [desc[0] for desc in cursor.description]
        
        serializable_results = []
        for row in result:
            s_row = [str(x) if x is not None else None for x in row]
            serializable_results.append(s_row)
            
        return jsonify({
            'columns': columns,
            'results': serializable_results
        })

    except Exception as e:
        error_msg = str(e)
        if "nodename nor servname provided" in error_msg:
            error_msg = ("DNS Resolution Failed. Are you disconnected from your VPN? "
                         "Please connect to the Rupeek VPN to access dw-redshift-prod.rupeek.com.")
        return jsonify({'error': error_msg}), 500
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    # Flask development server
    app.run(host='0.0.0.0', port=5000, debug=True)
