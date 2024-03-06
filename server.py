from flask import Flask, session, jsonify, render_template, redirect, url_for, request
import threading
import json
from pydantic import BaseModel
from src.objects import SpaceObj, EquipmentObj
from src.src_driver import ProcessDriver

import logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

#
app= Flask(__name__)

def driver(data):
    # print(data)
    equipments_json = data['equipments']
    space_json = data['spaces']
    equipments = []
    for e in equipments_json:
        x=EquipmentObj(**e)
        equipments.append(x)
    spaces = []
    for e in space_json:
        x=SpaceObj(**e)
        spaces.append(x)
    
    # spaces, equipments = data.spaces, data.equipments
    proc = ProcessDriver(spaces, equipments)
    eq = proc.output_equipment_in_space
    adj = proc.ns_edges
    print('process complete')
    return eq, adj

@app.route('/')
def init():
    return 'hello from revit - sql web service'

@app.route("/post_spaces_equipments", methods=['GET', 'POST'])
def post_spaces_equipments():
    print(f"request received with data")
    data = request.json
    proc_thread = threading.Thread(target=driver, args=(data,))
    proc_thread.start()
    #
    return {"message": "data received from revit client"}

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=7777, debug=True)
    

# flask --app server.py --debug run
# docker build --tag python-docker .
