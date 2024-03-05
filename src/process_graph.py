import numpy as np
import pandas as pd
from shapely import Point, Polygon
import mysql.connector

#
import logging
import json

# ------------------------------------------------------------------------------------ #
# ---------------------                         -------------------------------------- #
# ---------------------     process adjacency   -------------------------------------- #
# ---------------------                         -------------------------------------- #
# ------------------------------------------------------------------------------------ #

class ProcessGraph:
    def __init__(self, output_equipment_in_space_, df_spaces_, db_name_="revitmineABC"):
        self.output_equipment_in_space = output_equipment_in_space_
        self.df = pd.DataFrame(self.output_equipment_in_space)
        #
        self.database_name = db_name_
        x = self.df['project_name']
        c = x[0]
        c = c.replace(" ", "")
        c = c.replace(".", "_")
        self.table_name = c + "_adjacencies"
        # self.table_name = "adjacencies"
        self.project_name = c
        #
        self.space_ids = np.array(self.df['space_id'])
        self.space_names = np.array(self.df['space_name'])
        self.door_ids = np.array(self.df['equipment_uid'])

        # get edges to generate adjacencies plot the graph
        self.edge_ids = []
        for i, sid in enumerate(self.space_ids):
            eid_li = self.door_ids[i]
            for j, eid in enumerate(self.space_ids):
                if i != j:
                    eid_li2 = self.door_ids[j]
                    for k in eid_li:
                        if k in eid_li2:
                            a = [self.space_ids[j], self.space_ids[i]]
                            b = [self.space_ids[j], self.space_ids[i]]
                            if a not in self.edge_ids and b not in self.edge_ids:
                                self.edge_ids.append([self.space_ids[i], self.space_ids[j]])
        #
        # -- for poly intx 
        self.df_spaces = df_spaces_
        self.rx = np.array(self.df_spaces[['spaceId', 'coordinates']])
        connected_uids = self.get_connected_uids()  # in the form [a,b]
        for e in connected_uids:
            if e not in self.edge_ids:
                self.edge_ids.append(e)

        # run the adjacency process
        self.nodes_and_neighbors = {}
        for node in self.space_ids:
            self.find_node_neighbors(node)

        # data for each node : set of all connected neighbor nodes
        self.output_node_neighbors = {}
        for k, v in self.nodes_and_neighbors.items():
            x0 = ", ".join(self.nodes_and_neighbors[k])
            self.output_node_neighbors[k] = x0

        #
        self.ns_edges = []
        self.ns_edges_graph = []
        for i, e in enumerate(self.edge_ids):
            source_name = (self.space_from_uid(e[0]))
            source_sid = int(e[0])
            source_graph = source_name + str(e[0])
            target_name = self.space_from_uid(e[1])
            target_sid = int(e[1])
            target_graph = target_name + str(e[1])
            self.ns_edges.append([source_name, source_sid, target_name, target_sid, self.project_name])

        #
        # self.write_to_db()

    def get_node_from_uid(self, uid):
        space_name = ""
        for i, e in enumerate(self.space_ids):
            if e == uid:
                space_name = self.space_names[i]
                return space_name + "-" + str(uid)
        return space_name

    def find_node_neighbors(self, node):
        node_li = []
        for edge in self.edge_ids:
            a, b = edge[0], edge[1]
            if node == a or node == b:
                if a == node and b not in node_li:
                    x0 = self.get_node_from_uid(b)
                    node_li.append(x0)
                elif b == node and a not in node_li:
                    x0 = self.get_node_from_uid(a)
                    node_li.append(x0)
        if len(node_li) > 0:
            source_node_name = self.get_node_from_uid(node)
            self.nodes_and_neighbors[source_node_name] = node_li

    def get_ns_edges(self):
        return self.ns_edges

    def get_ns_edges_graph(self):
        return self.ns_edges_graph

    def space_from_uid(self, id):
        name = ""
        for i, e in enumerate(self.space_ids):
            if e == id:
                name = self.space_names[i]
                break
        return name

    # -------------------------------------------------------------#
    # -------------------- poly intx ------------------------------#
    # -------------------------------------------------------------#

    # get name from space id - poly intx
    def get_name_from_spaceid(self, sid):
        return [e for e in self.df_spaces[['spaceId', 'spaceFullName', 'level']].values if e[0] == str(sid)]

    #
    # get the level from space id of a space - poly intx
    def get_level_from_spaceid(self, spaceid):
        space_arr = self.df_spaces[['spaceId', 'level', 'spaceName']].values
        lvl = [e for e in space_arr if int(e[0]) == int(spaceid)]
        if len(lvl) > 0:
            return lvl[0]
        return

    #
    # get the xy of points from revit coordinates of a space id - poly intx    
    def get_xy_from_spaceid(self, spaceid):
        # coordinates of all spaces with one extra at the end
        # ry=[e for e in rx if e[0]==str(spaceid)][0] # error if id is not found in sapces
        # """
        ry = []
        for e in self.rx:
            if e[0] == str(spaceid):
                ry.append(e)
        ry = ry[0]
        if len(ry) == 0:
            print('space ids not found')
            return
            # """
        rc = np.array([float(e) for e in ry[1].split(',')[:-1]])  # coordinates of all spaces xyz
        rc2 = np.reshape(rc, (len(rc) // 3, 3))  # points xyz of the coordinates
        #
        rp = np.delete(rc2, 2, axis=1)  # remove the z from point - make it xy
        return rp

    def get_connected_uids(self):
        sid_arr = self.df_spaces['spaceId'].values  # get all space-ids as np array
        connected_uids = []  # in the form [a,b]
        for e in sid_arr:
            try:
                xy1 = self.get_xy_from_spaceid(e)  # function
                _, _, level1 = list(self.get_name_from_spaceid(e)[0])  # function
                poly1 = Polygon(xy1)
                # x1, y1=poly1.exterior.xy
                for f in sid_arr:
                    if e != f:
                        xy2 = self.get_xy_from_spaceid(f)  # function
                        _, _, level2 = list(self.get_name_from_spaceid(f)[0])  # function
                        poly2 = Polygon(xy2)
                        # x2, y2=poly2.exterior.xy
                        t = poly1.intersects(poly2)
                        t1 = [e, f] in connected_uids or [f, e] in connected_uids
                        if t == True and t1 == False and level1 == level2:
                            connected_uids.append([int(e), int(f)])
            except:
                pass
        return connected_uids

    # -------------------------------------------------------------#
    # -------------------- poly intx end  -------------------------#
    # -------------------------------------------------------------#
    def write_to_db(self):
        conn = mysql.connector.connect(host="127.0.0.1", user="root", password="toor")
        cur = conn.cursor()
        #
        try:
            f0 = f"CREATE DATABASE IF NOT EXISTS {self.database_name}"
            cur.execute(f0)
        except:
            pass
        #
        f1 = f"USE {self.database_name}"
        cur.execute(f1)
        #
        f2 = f"CREATE TABLE IF NOT EXISTS {self.table_name}(source_name VARCHAR(255), source_id INT(10), target_name VARCHAR(255), target_id INT(10), project_name VARCHAR(255))"
        cur.execute(f2)
        #
        f3 = f"INSERT INTO {self.table_name}(source_name, source_id, target_name, target_id, project_name) VALUES (%s, %s, %s, %s, %s)"
        cur.executemany(f3, self.ns_edges)
        conn.commit()
        conn.close()

    # DEPRECATED added all data to mysql db
    def write_to_db_deprecated(self):
        conn = mysql.connector.connect(host="127.0.0.1", user="root", password="toor")
        cur = conn.cursor()
        #
        f0 = "CREATE DATABASE IF NOT EXISTS {self.database_name}"
        cur.execute(f0)
        #
        f1 = "USE {self.database_name}"
        cur.execute(f1)
        #
        f2 = f"CREATE TABLE IF NOT EXISTS {self.table_name}(source VARCHAR(255), target VARCHAR(255))"
        cur.execute(f2)
        #
        f3 = f"INSERT INTO {self.table_name}(source, target) VALUES (%s, %s)"
        cur.executemany(f3, self.ns_edges)
        conn.commit()
        conn.close()
