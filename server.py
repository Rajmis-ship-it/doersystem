from flask import Flask, render_template, request, jsonify, session
import mysql.connector
from mysql.connector import pooling  # <-- PRO UPGRADE: Imported pooling!
from datetime import datetime        # <-- Required for the Date Math!
import os

# 1. Initialize the Flask App
app = Flask(__name__)
app.secret_key = "bhasin_secret_key_123" # Required for the Login Session

# -----------------------------------------------------
# 2. PRO UPGRADE: Database Connection Pool
# -----------------------------------------------------
# Python will open 5 connections right now and keep them alive 24/7.
dbconfig = {
    "host": "doer-database-bhasinmotors-5f09.j.aivencloud.com",
    "user": "avnadmin",
    "password": os.environ.get("DB_PASSWORD"),  # <--- CHANGED THIS LINE!
    "port": 14693,
    "database": "defaultdb"
}

print("🔄 Initializing Database Connection Pool...")
db_pool = pooling.MySQLConnectionPool(
    pool_name="bhasin_pool",
    pool_size=5,
    pool_reset_session=True,
    **dbconfig
)

def get_db_connection():
    # Instead of logging in from scratch, we just grab an open line from the pool!
    return db_pool.get_connection()

# -----------------------------------------------------
# 3. PAGE ROUTES (The Skeleton)
# -----------------------------------------------------

@app.route('/')
def index():
    # Shows the dashboard.html file from your templates folder
    return render_template('dashboard.html')

# -----------------------------------------------------
# 4. API ROUTES (The Brains)
# -----------------------------------------------------

# A. LOGIN ROUTE
@app.route('/login', methods=['POST'])
def login():
    data = request.json
    phone = data.get('phone')
    password = data.get('password')

    db = get_db_connection()
    cursor = db.cursor(dictionary=True)
    query = "SELECT * FROM admins WHERE phone = %s AND password = %s AND status = 'active'"
    cursor.execute(query, (phone, password))
    user = cursor.fetchone()
    db.close() # <-- This now simply returns the connection back to the pool!

    if user:
        return jsonify({"success": True, "user": user})
    else:
        return jsonify({"success": False, "message": "Invalid Phone or Password"})

# B. WEBHOOK RECEIVER (From Google Sheets)
@app.route('/webhook', methods=['POST'])
def receive_data():
    try:
        data = request.json
        db = get_db_connection()
        cursor = db.cursor()
        sql = """
        INSERT INTO scores (doer_name, doer_department, planned_date, actual_date, source_url)
        VALUES (%s, %s, %s, %s, %s)
        """
        values = (data.get('doer_name'), data.get('doer_department'), 
                  data.get('planned_date'), data.get('actual_date'), data.get('source_url'))
        cursor.execute(sql, values)
        db.commit() 
        cursor.close()
        db.close()
        return jsonify({"status": "success", "message": "Data saved to MySQL!"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# C. DATA FETCH & MATH ROUTE (For your 'Load Scores' button)
@app.route('/get_scores', methods=['GET'])
def get_scores():
    doer = request.args.get('doer', '')
    start = request.args.get('start', '')
    end = request.args.get('end', '')
    company = request.args.get('company', 'All')

    db = get_db_connection()
    cursor = db.cursor(dictionary=True)

    query = "SELECT * FROM scores WHERE 1=1"
    params = []

    if doer:
        query += " AND doer_name LIKE %s"
        params.append(f"%{doer}%")
    if start and end:
        query += " AND planned_date >= %s AND planned_date <= %s"
        params.extend([start, end])
    if company != 'All':
        query += " AND doer_department = %s"
        params.append(company)

    cursor.execute(query, params)
    raw_results = cursor.fetchall()
    db.close()

    # --- THE SCORING MATH ENGINE ---
    doer_stats = {}
    
    # Convert string dates to datetime objects for accurate comparison
    end_dt = datetime.strptime(end, '%Y-%m-%d').date() if end else None

    # 1. Group by Doer and Count
    for row in raw_results:
        d_name = row['doer_name']
        if d_name not in doer_stats:
            doer_stats[d_name] = {'planned': 0, 'actual': 0, 'onTime': 0}
        
        doer_stats[d_name]['planned'] += 1
        
        actual_date = row.get('actual_date')
        planned_date = row.get('planned_date')
        
        # --- THE FIX: Strip time off and normalize to pure Date objects ---
        if hasattr(actual_date, 'date'):
            actual_date = actual_date.date()
        elif isinstance(actual_date, str) and actual_date:
            try: actual_date = datetime.strptime(actual_date.split(' ')[0], '%Y-%m-%d').date()
            except ValueError: actual_date = None
            
        if hasattr(planned_date, 'date'):
            planned_date = planned_date.date()
        elif isinstance(planned_date, str) and planned_date:
            try: planned_date = datetime.strptime(planned_date.split(' ')[0], '%Y-%m-%d').date()
            except ValueError: planned_date = None
        # -----------------------------------------------------------------

        # Apply your exact Apps Script logic:
        if actual_date and end_dt and actual_date <= end_dt:
            doer_stats[d_name]['actual'] += 1
            if actual_date <= planned_date:
                doer_stats[d_name]['onTime'] += 1

    # 2. Calculate KRAs
    processed_results = []
    for d_name, stats in doer_stats.items():
        planned = stats['planned']
        actual = stats['actual']
        onTime = stats['onTime']
        
        kra1_val = ((actual - planned) / planned * 100) if planned > 0 else 0
        kra2_val = ((onTime - actual) / actual * 100) if actual > 0 else 0
        
        processed_results.append({
            'doer_name': d_name,
            'planned': planned,
            'actual': actual,
            'on_time': onTime,
            'kra1_pct': f"{kra1_val:.2f}%", 
            'kra2_pct': f"{kra2_val:.2f}%", 
            'kra1_num': kra1_val,           
            'kra2_num': kra2_val            
        })

    return jsonify(processed_results)

# -----------------------------------------------------
# 5. START THE ENGINE (Must be at the very bottom!)
# -----------------------------------------------------
if __name__ == '__main__':
    app.run(debug=True, port=5000)