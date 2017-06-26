from app import app, jsonify, request, time
import threading
import time
import bisect


microservices = {'127.0.0.1': 'null'}
global cont
regs={}
index_date= []
databases = {}

def worker():
	actual_time=time.time()
	global cont
	index = 0
	finish=0
	while (index < len(index_date)) and finish==0:
		if cont<index_date[index][0]:
			finish=1
		else:
			registros=databases[index_date[index][2]]
			del(registros[index_date[index][1]])
			index_date.pop(index)
		index=index+1
	cont=cont+1
	
	threading.Timer(5.0,worker).start()


threads = list()
t = threading.Thread(target=worker)
threads.append(t)
t.setDaemon(True)
cont=0
t.start()




@app.route('/database/<database_name>', methods=['POST'])
def create_database(database_name):
	if database_name in databases:
		return '',409
	else:
		databases[database_name] = {}
		ipn=request.remote_addr
		microservices[ipn]=database_name
		return '',204

@app.route('/database/<database_name>', methods=['GET'])
def get_database(database_name):
	if database_name in databases:
		ipn=request.remote_addr
		microservices[ipn]=database_name
		if len(databases[database_name])>0:
			regs=[reg for reg in databases[database_name].items()]
			return jsonify(regs),200
	return '',405



@app.route('/database/', methods=['GET'])
def get_databases():
	clave = request.args.get('key')
	if clave!=None:
		ipn=request.remote_addr
		database_id=microservices[ipn]
		database=databases[database_id]
		return jsonify(database[clave])
	else:
		keys=[key for key in databases.keys()]
		binddatabases={'databases':str(keys)}
		return jsonify(binddatabases)

@app.route('/database/i', methods=['GET'])
def get_index():
		return jsonify(index_date)
	

@app.route('/database/', methods=['POST'])
def add_register():
	ipn=request.remote_addr
	payload = request.get_json()
	database_id=microservices[ipn]
	if database_id in databases:
		bisect.insort(index_date, (payload['timestamp'],payload['key'],database_id))
		databases[database_id][payload['key']]=payload
		return str(payload['key'])
	return 'no existe la base de datos'




@app.route('/')
@app.route('/index')
def index():
    return "Hello, World!"

