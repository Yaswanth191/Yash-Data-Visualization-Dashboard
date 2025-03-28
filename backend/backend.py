from flask import Flask, request, jsonify
from flask_cors import CORS
import os
import json
import pandas as pd
from werkzeug.utils import secure_filename
import sqlite3
from datetime import datetime

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Configuration
UPLOAD_FOLDER = 'uploads'
DATABASE = 'data.db'
ALLOWED_EXTENSIONS = {'json', 'csv', 'xlsx', 'xls'}

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB limit

# Ensure upload folder exists
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Initialize database
def init_db():
    conn = sqlite3.connect(DATABASE)
    c = conn.cursor()
    
    # Create table if it doesn't exist
    c.execute('''
        CREATE TABLE IF NOT EXISTS data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            topic TEXT,
            sector TEXT,
            region TEXT,
            country TEXT,
            source TEXT,
            end_year TEXT,
            intensity INTEGER,
            likelihood INTEGER,
            relevance INTEGER,
            pest TEXT,
            swot TEXT,
            url TEXT,
            added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create index for better performance
    c.execute('CREATE INDEX IF NOT EXISTS idx_topic ON data (topic)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_country ON data (country)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_sector ON data (sector)')
    
    conn.commit()
    conn.close()

init_db()

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def process_json_file(file_path, append=False):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        if not isinstance(data, list):
            data = [data]
            
        return insert_data(data, append)
        
    except Exception as e:
        return {'error': f'Error processing JSON file: {str(e)}'}

def process_csv_file(file_path, append=False):
    try:
        df = pd.read_csv(file_path)
        data = df.to_dict('records')
        return insert_data(data, append)
        
    except Exception as e:
        return {'error': f'Error processing CSV file: {str(e)}'}

def process_excel_file(file_path, append=False):
    try:
        df = pd.read_excel(file_path)
        data = df.to_dict('records')
        return insert_data(data, append)
        
    except Exception as e:
        return {'error': f'Error processing Excel file: {str(e)}'}

def insert_data(data, append=False):
    conn = sqlite3.connect(DATABASE)
    c = conn.cursor()
    
    if not append:
        # Clear existing data if not appending
        c.execute('DELETE FROM data')
    
    count = 0
    for item in data:
        try:
            # Map the incoming data to our database schema
            c.execute('''
                INSERT INTO data (
                    title, topic, sector, region, country, 
                    source, end_year, intensity, likelihood, 
                    relevance, pest, swot, url
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                item.get('title'),
                item.get('topic'),
                item.get('sector'),
                item.get('region'),
                item.get('country'),
                item.get('source'),
                str(item.get('end_year')) if item.get('end_year') else None,
                item.get('intensity'),
                item.get('likelihood'),
                item.get('relevance'),
                item.get('pest'),
                item.get('swot'),
                item.get('url')
            ))
            count += 1
        except Exception as e:
            print(f"Error inserting record: {e}")
            continue
    
    conn.commit()
    conn.close()
    return {'count': count, 'message': f'Successfully processed {count} records'}

@app.route('/api/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
    
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)
        
        # Get upload option (replace or append)
        option = request.form.get('option', 'replace')
        append = option.lower() == 'append'
        
        # Process based on file type
        file_ext = filename.rsplit('.', 1)[1].lower()
        
        try:
            if file_ext == 'json':
                result = process_json_file(file_path, append)
            elif file_ext == 'csv':
                result = process_csv_file(file_path, append)
            elif file_ext in ('xlsx', 'xls'):
                result = process_excel_file(file_path, append)
            else:
                return jsonify({'error': 'Unsupported file type'}), 400
            
            if 'error' in result:
                return jsonify(result), 400
                
            return jsonify(result)
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
        finally:
            # Clean up - remove the uploaded file after processing
            try:
                os.remove(file_path)
            except:
                pass
    else:
        return jsonify({'error': 'File type not allowed'}), 400

@app.route('/api/data', methods=['GET'])
def get_data():
    # Pagination parameters
    limit = request.args.get('limit', default=None, type=int)
    offset = request.args.get('offset', default=0, type=int)
    
    conn = sqlite3.connect(DATABASE)
    c = conn.cursor()
    
    query = 'SELECT * FROM data'
    params = []
    
    # Add LIMIT and OFFSET if provided
    if limit is not None:
        query += ' LIMIT ? OFFSET ?'
        params.extend([limit, offset])
    
    c.execute(query, params)
    rows = c.fetchall()
    
    # Convert to list of dictionaries
    columns = [column[0] for column in c.description]
    result = [dict(zip(columns, row)) for row in rows]
    
    conn.close()
    return jsonify(result)

@app.route('/api/data/count', methods=['GET'])
def get_data_count():
    conn = sqlite3.connect(DATABASE)
    c = conn.cursor()
    
    c.execute('SELECT COUNT(*) FROM data')
    count = c.fetchone()[0]
    
    conn.close()
    return jsonify({'count': count})

@app.route('/api/delete', methods=['DELETE'])
def delete_data():
    conn = sqlite3.connect(DATABASE)
    c = conn.cursor()
    
    c.execute('DELETE FROM data')
    conn.commit()
    
    c.execute('SELECT COUNT(*) FROM data')
    remaining = c.fetchone()[0]
    
    conn.close()
    
    return jsonify({
        'message': 'All data deleted successfully',
        'remaining': remaining
    })

@app.route('/api/init', methods=['POST'])
def init_database():
    # Sample data for initialization
    sample_data = [
        {
            "title": "Climate change impacts on agriculture",
            "topic": "climate",
            "sector": "agriculture",
            "region": "North America",
            "country": "United States",
            "source": "NASA",
            "end_year": "2025",
            "intensity": 5,
            "likelihood": 3,
            "relevance": 4,
            "pest": "Environmental",
            "swot": "Threat",
            "url": "https://climate.nasa.gov"
        },
        {
            "title": "Renewable energy adoption trends",
            "topic": "energy",
            "sector": "utilities",
            "region": "Europe",
            "country": "Germany",
            "source": "IEA",
            "end_year": "2023",
            "intensity": 4,
            "likelihood": 5,
            "relevance": 5,
            "pest": "Technological",
            "swot": "Opportunity",
            "url": "https://www.iea.org"
        }
    ]
    
    result = insert_data(sample_data, append=False)
    return jsonify(result)

@app.route('/api/files', methods=['GET'])
def list_uploaded_files():
    try:
        files = []
        for filename in os.listdir(app.config['UPLOAD_FOLDER']):
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            if os.path.isfile(file_path):
                files.append({
                    'name': filename,
                    'size': os.path.getsize(file_path),
                    'modified': datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat()
                })
        return jsonify(files)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)