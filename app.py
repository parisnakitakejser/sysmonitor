from pysysadm import cpu, memory, disk
from pymongo import MongoClient
from bson.objectid import ObjectId
import datetime, time, pika, json, time, configparser, os

dir_path = os.path.dirname(os.path.realpath(__file__))
config = configparser.ConfigParser()
config.read(os.path.join(dir_path, 'config.ini'))

server_id = config['settings']['server_id']
server_key = config['settings']['server_key']

if config['settings']['database'] == 'mongodb':
    client = MongoClient('{hostname}:{port}'.format(hostname=config['mongodb']['hostname'], port=config['mongodb']['port']))
    db = client[config['mongodb']['database']]

def run():
    unix_timestamp = int(time.time())

    cpu_data = {
        'server-id' : server_id,
        'server-key' : server_key,
        'method' : 'cpu',
        'count' : cpu.count(),
        'procent' : cpu.procent(),
        'times' : cpu.times(),
        'timestamp' : unix_timestamp,
        'inserted_at' : datetime.datetime.utcnow()
    }

    memory_data = {
        'server-id' : server_id,
        'server-key' : server_key,
        'method' : 'memory',
        'status' : memory.status(),
        'timestamp' : unix_timestamp,
        'inserted_at' : datetime.datetime.utcnow()
    }

    partitions_data = {
        'server-id' : server_id,
        'server-key' : server_key,
        'method' : 'disk',
        'partitions' : disk.partitions(),
        'timestamp' : unix_timestamp,
        'inserted_at' : datetime.datetime.utcnow()
    }

    insert_data = []
    insert_data.append(cpu_data)
    insert_data.append(memory_data)
    insert_data.append(partitions_data)

    if config['settings']['database'] == 'mongodb':
        db['{prefix}queue-server-monitor'.format(prefix=config['mongodb']['prefix'])].insert(insert_data)

    return unix_timestamp

while(True):
    unix_timestamp = run()
    print('time {timestamp} is push to queue...'.format(timestamp=unix_timestamp))
    time.sleep(int(config['settings']['frequency']))
