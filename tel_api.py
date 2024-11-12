from flask import Flask, jsonify, request
import os
import json
import glob

app = Flask(__name__)

MODEL_RESULTS_DIR1 = 'model_1'
MODEL_RESULTS_DIR2 = 'model_2'
MODEL_RESULTS_DIR3 = 'model_3'

def read_json_lines(file_path):
    """Utility function to read JSON lines (one JSON object per line) and combine them into a JSON array."""
    all_data = []
    try:
        with open(file_path, 'r') as file:
            for line in file:
                data = json.loads(line.strip())  # Parse each line as a JSON object
                all_data.append(data)
        return all_data
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None

@app.route('/api/results/model_1', methods=['GET'])
def get_all_results1():
    """Get all results from all JSON files in model_1 directory."""
    all_results = {}
    
    # Search for all JSON files in the model_1 directory
    json_files = glob.glob(os.path.join(MODEL_RESULTS_DIR1, '*.json'))
    for file_path in json_files:
        model_name = os.path.splitext(os.path.basename(file_path))[0]  # Get model name from filename
        model_results = read_json_lines(file_path)  # Use read_json_lines to read JSON line format
        
        if model_results:
            all_results[model_name] = model_results
    
    if all_results:
        return jsonify(all_results), 200
    else:
        return jsonify({"message": "No results found."}), 404

@app.route('/api/results/model_2', methods=['GET'])
def get_all_results2():
    """Get all results from all JSON files in model_2 directory."""
    all_results = {}
    
    # Search for all JSON files in the model_2 directory
    json_files = glob.glob(os.path.join(MODEL_RESULTS_DIR2, '*.json'))
    for file_path in json_files:
        model_name = os.path.splitext(os.path.basename(file_path))[0]  # Get model name from filename
        model_results = read_json_lines(file_path)  # Use read_json_lines to read JSON line format
        
        if model_results:
            all_results[model_name] = model_results
    
    if all_results:
        return jsonify(all_results), 200
    else:
        return jsonify({"message": "No results found."}), 404

@app.route('/api/results/model_3', methods=['GET'])
def get_all_results3():
    """Get all results from all JSON files in model_3 directory."""
    all_results = {}
    
    # Search for all JSON files in the model_3 directory
    json_files = glob.glob(os.path.join(MODEL_RESULTS_DIR3, '*.json'))
    for file_path in json_files:
        model_name = os.path.splitext(os.path.basename(file_path))[0]  # Get model name from filename
        model_results = read_json_lines(file_path)  # Use read_json_lines to read JSON line format
        
        if model_results:
            all_results[model_name] = model_results
    
    if all_results:
        return jsonify(all_results), 200
    else:
        return jsonify({"message": "No results found."}), 404

@app.route('/api/results/model_1/humidity', methods=['GET'])
def get_results_by_humidity():
    """Get results for a specific humidity level."""
    humidity = request.args.get('humidity')
    if not humidity:
        return jsonify({"message": "Humidity level is required."}), 400
    
    try:
        humidity = float(humidity)  # Convert to float for comparison
    except ValueError:
        return jsonify({"message": "Invalid humidity level. Must be a number."}), 400
    
    all_results = []
    json_files = glob.glob(os.path.join(MODEL_RESULTS_DIR1, '*.json'))
    for file_path in json_files:
        model_results = read_json_lines(file_path)
        
        if model_results:
            # Filter results based on the humidity level
            filtered_results = [
                result for result in model_results 
                if result.get('humidity') == humidity
            ]
            
            # If any results match the humidity level, add them to all_results
            if filtered_results:
                all_results.extend(filtered_results)
    
    if all_results:
        return jsonify(all_results), 200
    else:
        return jsonify({"message": f"No results found for humidity {humidity}."}), 404

@app.route('/api/results/<model_name>', methods=['GET'])
def get_model_results_endpoint(model_name):
    """Get results for a specific model by file name."""
    model_file_path = os.path.join(MODEL_RESULTS_DIR1, f"{model_name}.json")
    model_results = read_json_lines(model_file_path)  # Use read_json_lines to read JSON line format
    
    if model_results:
        return jsonify(model_results), 200
    else:
        return jsonify({"message": f"Results for {model_name} not found."}), 404

@app.route('/api/results/<model_name>/device', methods=['GET'])
def get_device_results(model_name):
    """Get predictions for a specific device ID from a specific model."""
    device_id = request.args.get('device')
    if not device_id:
        return jsonify({"message": "Device ID is required."}), 400
    
    model_file_path = os.path.join(MODEL_RESULTS_DIR1, f"{model_name}.json")
    model_results = read_json_lines(model_file_path)
    
    if model_results:
        # Filter results based on the device ID
        filtered_results = [result for result in model_results if result.get('device') == device_id]
        if filtered_results:
            return jsonify(filtered_results), 200
        else:
            return jsonify({"message": f"No results found for device {device_id}."}), 404
    else:
        return jsonify({"message": f"Results for {model_name} not found."}), 404

@app.route('/api/results/<model_name>/timestamp', methods=['GET'])
def get_timestamp_results(model_name):
    """Get predictions for a specific timestamp from a specific model."""
    timestamp = request.args.get('ts')
    if not timestamp:
        return jsonify({"message": "Timestamp (ts) is required."}), 400
    
    model_file_path = os.path.join(MODEL_RESULTS_DIR1, f"{model_name}.json")
    model_results = read_json_lines(model_file_path)
    
    if model_results:
        # Filter results based on the timestamp
        filtered_results = [result for result in model_results if result.get('ts') == timestamp]
        if filtered_results:
            return jsonify(filtered_results), 200
        else:
            return jsonify({"message": f"No results found for timestamp {timestamp}."}), 404
    else:
        return jsonify({"message": f"Results for {model_name} not found."}), 404

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
