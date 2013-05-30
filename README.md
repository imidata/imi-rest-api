imi-rest-api
============

Authenticated REST API to access IMI Data


Install the development environment
------------

    git clone git@github.com:jonimidata/imi-rest-api.git
    cd imi-rest-api
    cp env.example .env 
    nano -w .env 
    virtualenv venv --distribute
    . venv/bin/activate
    pip install -r requirements.txt
    deactivate


Developing
-------------

    cd imi-rest-api
    . venv/bin/activate
	foreman start	
