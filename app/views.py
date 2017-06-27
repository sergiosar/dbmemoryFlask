from app import app, jsonify, request, time
import threading
import time
import bisect


microservices = {'127.0.0.1': 'null','127.0.0.1': 'null'}
cont=0
regs={}
index_date= []
databases = {}
buff=[]
MAX = 10
condition=threading.Condition()

def worker():
	actual_time=time.time()
	global cont
	index = 0
	finish=0
	while (index < len(index_date)) and finish==0:
		if cont<index_date[index][0]:
			finish=1
		else:
			condition.acquire()
			registros=databases[index_date[index][2]]
			del(registros[index_date[index][1]])
			index_date.pop(index)
			condition.release()
		index=index+1
	cont=cont+1
	threading.Timer(5.0,worker).start()

def worker_addregister():
	global buff
	while True:
		condition.acquire()
		if len(buff)==0:
			condition.wait()
		else:
			tupla=buff[-1]
			bisect.insort(index_date, tupla)
			databases[tupla[2]][tupla[1]]=tupla[3]
			buff.pop(-1)
		condition.notify()
		condition.release()
		time.sleep(1)


threads = list()
t = threading.Thread(target=worker)
t2 = threading.Thread(target=worker_addregister)
threads.append(t)
threads.append(t2)
t.setDaemon(True)
t2.setDaemon(True)
cont=0
t.start()
t2.start()




@app.route('/database/<database_name>', methods=['POST'])
def create_database(database_name):
	ipn=request.remote_addr
	if ipn in microservices:
		if database_name in databases:
			return '',409
		else:
			databases[database_name] = {}
			microservices[ipn]=database_name
			return '',204
	else:
		return '',401

@app.route('/database/<database_name>', methods=['GET'])
def get_database(database_name):
	ipn=request.remote_addr
	if ipn in microservices:
		if database_name in databases:
			microservices[ipn]=database_name
			if len(databases[database_name])>0:
				regs=[reg for reg in databases[database_name].items()]
				return jsonify(regs),200
			else:
				return '[]',200
		return '',405
	else:
		return '',401




@app.route('/database/', methods=['GET'])
def get_databases():
	ipn=request.remote_addr
	if ipn in microservices:
		clave = request.args.get('key')
		if clave!=None:
			database_id=microservices[ipn]
			database=databases[database_id]
			if clave in database:
				return jsonify(database[clave]),200
			else:
				return '', 404
		else:
			keys=[key for key in databases.keys()]
			binddatabases={'databases':str(keys)}
			return jsonify(binddatabases)
	else:
		return '',401


@app.route('/databasei/', methods=['GET'])
def get_index():
		return jsonify(index_date)
	

@app.route('/database/', methods=['POST'])
def add_register():
	ipn=request.remote_addr
	if ipn in microservices:
		payload = request.get_json()
		database_id=microservices[ipn]
		if database_id in databases:
			global buff
			condition.acquire()
			buff.append((payload['timestamp'],payload['key'],database_id,payload))
			condition.notify()
			condition.release()
			return str('registro agregado: '+payload['key']),200
		return 'no existe la base de datos'
	else:
		return '',401




@app.route('/')
@app.route('/index')
def index():
    return "init"



