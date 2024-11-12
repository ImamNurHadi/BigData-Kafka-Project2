from flask import Flask, jsonify, request
import os
import json
import glob

app = Flask(__name__)

MODEL_RESULTS_DIR = {
    'model_1': 'model_1',
    'model_2': 'model_2',
    'model_3': 'model_3'
}

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

@app.route('/api/results/<model_name>/filter', methods=['POST'])
def get_results_by_feature_range(model_name):
    """Get predictions within specific ranges for various features in any model using POST."""
    
    # Ensure the model_name is valid
    if model_name not in MODEL_RESULTS_DIR:
        return jsonify({"message": f"Model {model_name} not found."}), 404
    
    # Read the JSON data from the POST request body
    feature_filters = request.json
    if not feature_filters:
        return jsonify({"message": "Feature filter criteria must be provided in the request body."}), 400

    predictions = []
    model_dir = MODEL_RESULTS_DIR[model_name]
    json_files = glob.glob(os.path.join(model_dir, '*.json'))
    
    # Process each file in the specified model directory
    for file_path in json_files:
        model_results = read_json_lines(file_path)
        
        if model_results:
            # Filter results based on feature ranges and collect only `prediction` values
            filtered_predictions = [
                {"prediction": result["prediction"]}
                for result in model_results 
                if all(
                    feature_filters[feature].get('min', float('-inf')) <= result.get(feature, float('inf')) <= feature_filters[feature].get('max', float('inf'))
                    for feature in feature_filters
                )
            ]
            
            # If any predictions match the feature ranges, add them to predictions list
            if filtered_predictions:
                predictions.extend(filtered_predictions)
    
    if predictions:
        return jsonify(predictions), 200
    else:
        return jsonify({"message": "No results found for the specified feature ranges."}), 404

@app.route('/api/results/<model_name>', methods=['GET'])
def get_all_results(model_name):
    """Get all results from all JSON files in the specified model directory."""
    
    # Ensure the model_name is valid
    if model_name not in MODEL_RESULTS_DIR:
        return jsonify({"message": f"Model {model_name} not found."}), 404

    all_results = {}
    model_dir = MODEL_RESULTS_DIR[model_name]
    json_files = glob.glob(os.path.join(model_dir, '*.json'))
    
    # Process each file in the specified model directory
    for file_path in json_files:
        model_name = os.path.splitext(os.path.basename(file_path))[0]  # Get model name from filename
        model_results = read_json_lines(file_path)
        
        if model_results:
            all_results[model_name] = model_results
    
    if all_results:
        return jsonify(all_results), 200
    else:
        return jsonify({"message": "No results found."}), 404

@app.route('/api/recommendations/weather', methods=['POST'])
def get_weather_recommendations():
    """Get recommendations for places based on weather conditions such as low temperature and humidity."""
    
    # Read the weather conditions from the POST request body
    weather_conditions = request.json
    if not weather_conditions:
        return jsonify({"message": "Weather condition criteria must be provided in the request body."}), 400
    
    # Extract temperature and humidity range from the request body
    temperature_min = weather_conditions.get('temperature_min', float('-inf'))
    temperature_max = weather_conditions.get('temperature_max', float('inf'))
    humidity_min = weather_conditions.get('humidity_min', float('-inf'))
    humidity_max = weather_conditions.get('humidity_max', float('inf'))

    recommendations = []
    
    # Iterate through all model directories and filter based on the weather conditions
    for model_name, model_dir in MODEL_RESULTS_DIR.items():
        json_files = glob.glob(os.path.join(model_dir, '*.json'))
        
        for file_path in json_files:
            model_results = read_json_lines(file_path)
            
            if model_results:
                # Filter results based on the weather conditions (temperature and humidity)
                filtered_recommendations = [
                    {
                        "device": result["device"],
                        "humidity": result["humidity"],
                        "temp": result["temp"]
                    }
                    for result in model_results
                    if temperature_min <= result.get('temp', float('inf')) <= temperature_max and
                    humidity_min <= result.get('humidity', float('inf')) <= humidity_max
                ]
                
                if filtered_recommendations:
                    recommendations.extend(filtered_recommendations)
    
    if recommendations:
        return jsonify(recommendations), 200
    else:
        return jsonify({"message": "No recommendations found for the specified weather conditions."}), 404


# Additional endpoints would go here as needed

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
