from flask import Flask, request, jsonify
from module.database import Database

app = Flask(__name__)
db = Database()

@app.route('/')
def index():
    return "home"

@app.route("/productos/venDep", methods = ['GET'])
def productosvenDep():
    if request.method == 'GET' :
        try:
            result = db.readVenDep()
        except Exception as e:
            return e
    return jsonify(result)

@app.route("/productos/catCom", methods = ['GET'])
def productoscatCom():
    if request.method == 'GET' :
        try:
            result = db.readcatCom()
        except Exception as e:
            return e
    return jsonify(result)

@app.route("/clientes/cliCom", methods = ['GET'])
def clientescliCom():
    if request.method == 'GET' :
        try:
            result = db.readcliCom(request.json["top"])
        except Exception as e:
            return e
    return jsonify(result)


if __name__=="main":
    app.debug = True
    app.run()

# cd "G:\Mi unidad\DataEngineer\python\NoteBooks\04 Proyecto\API"
# set FLASK_APP=app.py
# python -m flask run 

