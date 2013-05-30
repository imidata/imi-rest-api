import os
from flask import Flask

app = Flask(__name__)
DEBUG = os.getenv('DEBUG',False)

@app.route('/')
def hello():
    return 'Hello World!'

if __name__ == '__main__':
    app.run(debug=DEBUG)
