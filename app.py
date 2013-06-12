import os
from flask import Flask, render_template, url_for, jsonify, g, request
import psycopg2
import imimodel

app = Flask(__name__)
DEBUG = os.getenv('DEBUG',False)
DATABASE_URL = os.getenv('DATABASE_URL',None)


@app.before_request
def before_request():
	g.db = imimodel.ImiModel(DATABASE_URL)

@app.teardown_request
def teardown_request(exception):
    g.db.close()

@app.route('/')
def hello():
    return render_template('index.html', database=DATABASE_URL)

@app.route('/1/products')
@app.route('/1/products/<product_id>')
def products(product_id=None):
	if product_id:
		product = g.db.product(product_id)
		return jsonify(product)
	elif request.args.get('category', None):
		products = g.db.product_list(category=request.args.get('category', None))
	else:		
		products = g.db.product_list()

	to_return = []
	for p in products['results']:
		to_return.append({
			"productId":p[0],
			"description":p[1],
			"type":p[2],
			"category":p[3],
			"extended":p[4]
			})


	return jsonify(products=to_return)


@app.route('/1/demand')
def demand():
	group_by=str(request.args.get('group_by', None))
	products=str(request.args.get('products', None))
	geo_filter=str(request.args.get('geo', None))
	products = products.split(",")
	result = g.db.demand(group_by=group_by,geo_filter=str(geo_filter),products=products)
	app.logger.debug(result)
	return jsonify(result)


@app.route('/1/location/<duns>')
def location(duns=None):
	products=str(request.args.get('products', None))
	try: 
		if products == 'None':
			result = g.db.location_demand(duns=duns)
		else:
			products = products.split(",")
			result = g.db.location_demand(duns=duns,products=products)
		return jsonify(result)
	except:
		response = jsonify(type="error",message="invalid duns number",)
		response.status_code = 422
		return response


if __name__ == '__main__':
	print "local"
	app.run(debug=DEBUG)
