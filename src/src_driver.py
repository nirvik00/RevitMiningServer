import os

import numpy as np
import pandas as pd
from shapely import Point, Polygon
import json

from src.get_equipments_in_space import GetEquipmentsInSpaces as geis
from src.process_graph import ProcessGraph as pgr

import logging

# logging.basicConfig(filename="revit_mining.log", format='%(asctime)s %(message)s', filemode='w',level=logging.DEBUG)
logging.basicConfig(format='%(asctime)s %(message)s', filemode='w', level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


# import networkx as nx
# import matplotlib.pyplot as plt
# import time

# ------------------------------------------------------------------------------------ #
# ---------------------                         -------------------------------------- #
# ---------------------     driver              -------------------------------------- #
# ---------------------                         -------------------------------------- #
# ------------------------------------------------------------------------------------ #

class ProcessDriver:
    def __init__(self, space_data, equipment_data):
        self.output_equipment_in_space = []
        self.ns_edges = []
        self.database_name = "revitmine002"

        # find equipments in spaces
        g = geis(space_data, equipment_data, self.database_name)

        self.output_equipment_in_space = g.get_equipment_in_space()
        # with open("abcd.json", "w") as f:
        # json.dump(self.output_equipment_in_space, f)

        # adjacency process
        p = pgr(self.output_equipment_in_space, g.df_spaces, self.database_name)
        self.ns_edges = p.output_node_neighbors
        # with open("efgh.json", "w") as f:
        # json.dump(self.ns_edges, f)

        g.write_to_db_equipments()
        g.write_to_db_spaces()
        p.write_to_db()

        try:
            pass
        except Exception as e:
            print("error writing to db")
            print(e)

# ------------------------------------------------------------------------- #
# ---------------------                     ------------------------------- #
# ---------------------     end             ------------------------------- #
# ---------------------                     ------------------------------- #
# ------------------------------------------------------------------------- #
