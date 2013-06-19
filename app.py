import os
from flask import Flask, render_template, url_for, jsonify, g, request, make_response, current_app
import psycopg2
import imimodel
from datetime import timedelta
from functools import update_wrapper

app = Flask(__name__)
DEBUG = os.getenv('DEBUG',False)
DATABASE_URL = os.getenv('DATABASE_URL',None)


def crossdomain(origin=None, methods=None, headers=None,
                max_age=21600, attach_to_all=True,
                automatic_options=True):
    if methods is not None:
        methods = ', '.join(sorted(x.upper() for x in methods))
    if headers is not None and not isinstance(headers, basestring):
        headers = ', '.join(x.upper() for x in headers)
    if not isinstance(origin, basestring):
        origin = ', '.join(origin)
    if isinstance(max_age, timedelta):
        max_age = max_age.total_seconds()

    def get_methods():
        if methods is not None:
            return methods

        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        def wrapped_function(*args, **kwargs):
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))
            if not attach_to_all and request.method != 'OPTIONS':
                return resp

            h = resp.headers

            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            if headers is not None:
                h['Access-Control-Allow-Headers'] = headers
            return resp

        f.provide_automatic_options = False
        return update_wrapper(wrapped_function, f)
    return decorator



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
@crossdomain(origin='*')
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
@crossdomain(origin='*')
def demand():
	group_by=str(request.args.get('group_by', None))
	products=str(request.args.get('products', None))
	geo_filter=str(request.args.get('geo', None))
	products = products.split(",")
	result = g.db.demand(group_by=group_by,geo_filter=str(geo_filter),products=products)
	app.logger.debug(result)
	return jsonify(result)


@app.route('/1/location/<duns>')
@crossdomain(origin='*')
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
