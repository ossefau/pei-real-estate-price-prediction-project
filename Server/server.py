from flask import Flask, request, jsonify
import util

app = Flask(__name__)

@app.route('/get_location_names', methods=['GET'])
def get_location_names():
    response = jsonify({
        'locations': util.get_location_names()
    })
    response.headers.add('Access-Control-Allow-Origin', '*')

    return response


@app.route('/get_property_type_names', methods=['GET'])
def get_property_type_names():
    response = jsonify({
        'property_type_names': util.get_property_types()
    })
    response.headers.add('Access-Control-Allow-Origin', '*')

    return response



@app.route('/get_attachment_types', methods=['GET'])
def get_attachment_names():
    response = jsonify({
        'attachment_type_names': util.get_attachement_types()
    })
    response.headers.add('Access-Control-Allow-Origin', '*')

    return response


@app.route('/predict_home_price', methods=['GET', 'POST'])
def predict_home_price():
    sqft = float(request.form['sqft'])
    location = request.form['location']
    bed = int(request.form['bed'])
    bath = int(request.form['bath'])
    property_type = request.form['property_type']
    attachement = request.form['attachement']

    response = jsonify({
        'estimated_price': util.get_estimated_price(location,sqft,bed,bath,property_type,attachement)
    })
    response.headers.add('Access-Control-Allow-Origin', '*')

    return response

if __name__ == "__main__":
    print("Starting Python Flask Server For Home Price Prediction...")
    util.load_saved_artifacts()
    app.run()