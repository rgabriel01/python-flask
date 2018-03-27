import ipdb
import psycopg2
import calendar
import time
from datetime import datetime
from flask import Flask, jsonify

app = Flask(__name__)

@app.route("/<string:device_id>/<string:epoch_time>", methods=['POST'])
def post(device_id,epoch_time):
    device_id_str = str(device_id)
    epoch_time_str = str(epoch_time)
    conn = psycopg2.connect("dbname='tanda' user='raymondgabriel' host='localhost'")
    cur = conn.cursor()
    cur.execute("insert into ping (device_id, epoch_time) values ('%s', '%s')" % (device_id_str, epoch_time_str))
    conn.commit()
    conn.close()
    return jsonify({'success': 'success'}), 200

@app.route("/clear_data", methods=['POST'])
def clear_data():
    conn = psycopg2.connect("dbname='tanda' user='raymondgabriel' host='localhost'")
    cur = conn.cursor()
    cur.execute("delete from ping")
    conn.commit()
    conn.close()
    return jsonify({'success': 'success'}), 200

@app.route("/devices", methods=['GET'])
def devices():
    conn = psycopg2.connect("dbname='tanda' user='raymondgabriel' host='localhost'")
    cur = conn.cursor()
    cur.execute("select distinct device_id from ping")
    rows = cur.fetchall()
    devices = []
    for row in rows:
        devices.append(row[0])
    conn.close()
    return jsonify(devices), 200

@app.route("/<string:device_id>/<string:query_date>", methods=['GET'])
def get_data(device_id, query_date):
    device_id_str = str(device_id)
    query_date_str = str(query_date) + ' 00:00:00'
    query_date_str_limit = str(query_date) + ' 23:59:59'
    from_epoch = get_unix_time(query_date_str)
    to_epoch = get_unix_time(query_date_str_limit)

    conn = psycopg2.connect("dbname='tanda' user='raymondgabriel' host='localhost'")
    cur = conn.cursor()
    cmd = ''
    if device_id_str != 'all':
        cmd = "select * from ping where epoch_time >= %s and epoch_time <= %s and device_id = '%s'" % (from_epoch, to_epoch, device_id_str)
    else:
        cmd = "select * from ping where epoch_time >= %s and epoch_time <= %s" % (from_epoch, to_epoch)

    cur.execute(cmd)
    rows = cur.fetchall()
    if device_id_str != 'all':
        devices = []
        for row in rows:
            devices.append(row[2])
    else:
        devices = {}
        for row in rows:
            id,device_id, timestamp = row
            if device_id not in devices:
                devices[device_id] = []
            devices[device_id].append(timestamp)
    conn.close()
    return jsonify(devices), 200

@app.route("/<string:device_id>/<string:query_from_date>/<string:query_to_date>", methods=['GET'])
def get_ranged_data(device_id, query_from_date, query_to_date):
    device_id_str = str(device_id)
    query_from_string = get_unix_time(str(query_from_date) + " 00:00:00") if iso_formatted_date(str(query_from_date)) else str(query_from_date)
    query_to_string = get_unix_time(str(query_to_date) + " 23:59:59") if iso_formatted_date(str(query_to_date)) else str(query_to_date)

    conn = psycopg2.connect("dbname='tanda' user='raymondgabriel' host='localhost'")
    cur = conn.cursor()
    cmd = ''
    if device_id_str != 'all':
        cmd = "select * from ping where epoch_time >= %s and epoch_time < %s and device_id = '%s'" % (query_from_string, query_to_string, device_id_str)
    else:
        cmd = "select * from ping where epoch_time >= %s and epoch_time < %s" % (query_from_string, query_to_string)

    cur.execute(cmd)
    rows = cur.fetchall()
    if device_id_str != 'all':
        devices = []
        for row in rows:
            devices.append(row[2])
    else:
        devices = {}
        for row in rows:
            id,device_id, timestamp = row
            if device_id not in devices:
                devices[device_id] = []
            devices[device_id].append(timestamp)
    conn.close()
    return jsonify(devices), 200

# private stuff
def get_unix_time(date_string):
    return int(time.mktime(time.strptime(date_string, '%Y-%m-%d %H:%M:%S'))) - time.timezone

def iso_formatted_date(date_string):
    return "-" in date_string
