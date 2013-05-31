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




if __name__ == '__main__':
    app.run(debug=DEBUG)
