import numpy as np
import pandas as pd
from shapely import Point, Polygon
import mysql.connector
#
import logging
import json

# import time

logging.basicConfig(filename="revit_mining.log", format='%(asctime)s %(message)s', filemode='w', level=logging.DEBUG)
# logging.basicConfig(format='%(asctime)s %(message)s', filemode='w',level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


# ------------------------------------------------------------------------------------ #
# ---------------------                         -------------------------------------- #
# ---------------------     process revit data  -------------------------------------- #
# ---------------------                         -------------------------------------- #
# ------------------------------------------------------------------------------------ #


class GetEquipmentsInSpaces:
    def __init__(self, space_data, equipment_data, db_name_="revitmineABC"):
        s_li = []
        for e in space_data:
            x = [e.category, e.spaceName, e.spaceFullName, e.level, e.area, e.uniqueIdentifier, e.spaceId, e.bounds, e.coordinates, e.projectName]
            s_li.append(x)
        s_cols = ["category", "spaceName", "spaceFullName", "level", "area", "uniqueIdentifier", "spaceId", "bounds", "coordinates", "projectName"]
        self.df_spaces = pd.DataFrame(s_li, columns=s_cols)

        e_li = []
        for e in equipment_data:
            x = [e.category, e.equipmentName, e.equipmentType, e.description, e.min, e.max, e.centroid, e.level, e.uniqueIdentifier]
            e_li.append(x)
        e_cols = ["category", "equipmentName", "equipmentType", "description", "min", "max", "centroid", "level", "uniqueIdentifier"]
        self.df_equipments = pd.DataFrame(e_li, columns=e_cols)

        #
        logger.info(self.df_spaces[['category', 'spaceName', 'spaceId', 'uniqueIdentifier', 'projectName']])
        self.database_name = db_name_

        # get equipments for all spaces
        self.space_id_equipment_names = {}
        self.space_id_space_name = {}
        self.output_equipment_in_space = []

        x = self.df_spaces['projectName'].values
        c = x[0]
        c = c.replace(" ", "")
        c = c.replace(".", "_")
        self.table_name_equipments = c + "_equipments"
        self.table_name_spaces = c + "_spaces"
        # self.table_name_equipments = "equipments"
        # self.table_name_spaces = "spaces"

        # iterate over all space, 
        # find equipments in each space
        # generate a list of equipments in spaces 
        num = len(self.df_spaces)
        rp, rn, rl, sid, ec, el, et, eid, en, pn, ar = self.get_space_data()
        if len(rp) == 0:
            pass
        else:
            ir = 0
            while ir < len(self.df_spaces):
                if ir % 100 == 0:
                    print(f"space: {ir} out of {num}")
                try:
                    irn = rn[ir]  # space_name_index
                    irl = rl[ir]  # space_level_index
                    irid = sid[ir]  # space_id_index
                    irp = rp[ir]  # polygons
                    irar = ar[ir]  # area at index/ iteration
                    irar2 = float(irar.split(" ")[0])
                    if irar2 > 0:
                        self.check_equipments(ir, irp, irn, irl, irid, ec, el, et, eid, en, pn, irar2)
                    else:
                        pass
                except Exception as e:
                    pass
                ir += 1

    # step 2a: transform the points to check containment
    def get_equipment_points_to_check(self, uid, irp):
        ex = np.array(self.df_equipments[['uniqueIdentifier', 'centroid', 'min', 'max']])
        ey = [e for e in ex if e[0] == uid][0]
        ep, em, en = np.array([float(e) for e in ey[1].split(',')[:-1]]), np.array(
            [float(e) for e in ey[2].split(',')[:-1]]), np.array([float(e) for e in ey[3].split(',')[:-1]])
        sc = 1.05
        ep0 = [ep[0] - (ep[0] - en[0]) * sc, ep[1]]
        ep1 = [ep[0] + (ep[0] - en[0]) * sc, ep[1]]
        ep2 = [ep[0], ep[1] - (ep[1] - en[1]) * sc]
        ep3 = [ep[0], ep[1] + (ep[1] - en[1]) * sc]
        a, b, c, d = Point(ep0), Point(ep1), Point(ep2), Point(ep3)
        return np.any(np.array([a.within(irp), b.within(irp), c.within(irp), d.within(irp)]))

    # step 2: check equipment in spaces
    def check_equipments(self, ir, irp, irn, irl, irid, ec, el, et, eid, en, pn, irar):
        eq = []
        eqn = []
        eql = []  # equipment location
        for i, e in enumerate(ec):
            p = np.array(e)  # coordinates of equipment
            sloc = str(p[0]) + ", " + str(p[1]) + ", " + str(p[2])  # location of equipment in string
            ez = p[2]  # equipment_z
            pt = Point(p[:2])  # equipment_loc
            t1 = pt.within(irp)
            t2 = irl in el[i] or el[i] in irl
            if et[i] == "door":
                t3 = self.get_equipment_points_to_check(eid[i], irp)
                if t2 and t3 and eid[i] not in eq:
                    eq.append(eid[i])
                    eqn.append(et[i] + "_" + en[i])
                    eql.append(sloc)
            elif t1 and t2:
                # print(f"OK {en[i]} __ {et[i]} __ {rn[ir]}__{irl}")
                if eid[i] not in eq:
                    eq.append(eid[i])
                    eqn.append(et[i] + "_" + en[i])
                    eql.append(sloc)
        self.space_id_equipment_names[irid] = eqn
        self.space_id_space_name[irid] = irn
        x = {'space_id': int(irid), 'space_level': irl, 'space_name': irn, 'equipment': eqn, 'equipment_uid': eq,
             'space_area': irar, 'project_name': pn, 'equipment_location': eql}
        # print(x)
        self.output_equipment_in_space.append(x)

    # step 1 get spaces and equipment data to check
    def get_space_data(self):
        # ei=[]
        rn = self.df_spaces['spaceFullName']  # space_names
        sid = self.df_spaces['spaceId']  # space_id
        rl = self.df_spaces['level']  # space_levels
        pn = self.df_spaces['projectName'][0]  # project_name
        ar = []
        for e in self.df_spaces['area']:
            f = e.split(" ")[0]
            g = f.replace(",", "")
            ar.append(g)
        # equipments
        eid = self.df_equipments['uniqueIdentifier']  # equipment_unique_identifier
        en = self.df_equipments['equipmentName']  # equipment_name
        et = self.df_equipments['equipmentType']  # equipment_type
        ec = [[float(f) for f in e.split(',')] for e in self.df_equipments['centroid']]  # equipment coordinates
        el = self.df_equipments['level']
        # ec=np.array([[float(f) for f in e.split(',')] for e in self.df_equipments['min']])#equipment coordinates
        from shapely import Point, Polygon
        rp = []  # room polygons
        for e in self.df_spaces['coordinates']:
            try:
                c = np.array([float(f) for f in e.split(',')[:-1]])
                c2 = np.reshape(c, (len(np.array([float(f) for f in e.split(',')[:-1]])) // 3, 3))
                c2[:, 2] = 0
                rp.append(Polygon(c2))
            except:
                pass
        return rp, rn, rl, sid, ec, el, et, eid, en, pn, ar

    # DEPRECATED step 2: check equipment in spaces
    def check_equipments_deprecated(self, ir, irp, irn, irl, irid, ec, el, et, eid, en, irpn, irar):
        eq = []
        eqn = []
        for i, e in enumerate(ec):
            p = np.array(e)  # coordinates of equipment
            ez = p[2]  # equipment_z
            pt = Point(p[:2])  # equipment_loc
            t1 = pt.within(irp)
            t2 = irl in el[i] or el[i] in irl
            if et[i] == "door":
                t3 = self.get_equipment_points_to_check(eid[i], irp)
                if t2 and t3 and eid[i] not in eq:
                    eq.append(eid[i])
                    eqn.append(et[i] + "_" + en[i])
            elif t1 and t2:
                # print(f"OK {en[i]} __ {et[i]} __ {rn[ir]}__{irl}")
                if eid[i] not in eq:
                    eq.append(eid[i])
                    eqn.append(et[i] + "_" + en[i])
        self.space_id_equipment_names[irid] = eqn
        self.space_id_space_name[irid] = irn
        x = {}
        x['space_id'], x['space_level'], x['space_name'], x['equipments'], x['equipments_uid'], x['space_area'], x['project_name'] = int(irid), irl, irn, eqn, eq, irar, irpn
        self.output_equipment_in_space.append(x)

    # DEPRECATED step 1 get spaces and equipment data to check
    def get_space_data_deprecated(self, ir):
        # ei=[]
        rn = self.df_spaces['spaceFullName']  # space_names
        sid = self.df_spaces['spaceId']  # space_id
        rl = self.df_spaces['level']  # space_levels
        irn = rn[ir]  # space_name_index
        irl = rl[ir]  # space_level_index
        irid = sid[ir]  # space_id_index
        pn = self.df_spaces['projectName']  # project_name
        irpn = pn[ir]
        ar = []  # area list
        for e in self.df_spaces['area']:
            ar0 = e.split(" ")[0]
            ar1 = ar0.replace(',', '')
            ar.append(ar1)
        # ar = [float(e.split(" ")[0]) for e in self.df_spaces['area']]  # area
        irar = ar[ir]  # area at index/ iteration
        #
        # equipments
        eid = self.df_equipments['uniqueIdentifier']  # equipment_unique_identifier
        en = self.df_equipments['equipmentName']  # equipment_name
        et = self.df_equipments['equipmentType']  # equipment_type
        ec = [[float(f) for f in e.split(',')] for e in self.df_equipments['centroid']]  # equipment coordinates
        el = self.df_equipments['level']
        # ec=np.array([[float(f) for f in e.split(',')] for e in self.df_equipments['min']])#equipment coordinates
        from shapely import Point, Polygon
        rp = []  # room polygons
        for e in self.df_spaces['coordinates']:
            c = np.array([float(f) for f in e.split(',')[:-1]])
            c2 = np.reshape(c, (len(np.array([float(f) for f in e.split(',')[:-1]])) // 3, 3))
            c2[:, 2] = 0
            rp.append(Polygon(c2))
        irp = rp[ir]
        self.check_equipments(ir, irp, irn, irl, irid, ec, el, et, eid, en, irpn, irar)

    # main data that works
    def get_equipment_in_space(self):
        return self.output_equipment_in_space

    # added all data to mysql db
    def write_to_db_equipments(self):
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
        f2 = f"CREATE TABLE IF NOT EXISTS {self.table_name_equipments}(equipment_id VARCHAR(255), equipment_name VARCHAR(255), equipment_location VARCHAR(255), space_id INTEGER, project_name VARCHAR(255))"
        # f2 = f"CREATE TABLE IF NOT EXISTS equipments(equipment_id VARCHAR(255), equipment_name VARCHAR(255), equipment_location VARCHAR(255), space_id INTEGER, project_name VARCHAR(255))"

        cur.execute(f2)
        data_arr = []
        #
        for x in self.output_equipment_in_space:
            a, b = x['space_id'], x['project_name']
            if len(x['equipment_uid']) == 0 or x['equipment_location'] == 0:
                continue
            else:
                uid_arr = x['equipment_uid']
                name_arr = x['equipment']
                loc_arr = x['equipment_location']
                for i, e in enumerate(uid_arr):
                    m = uid_arr[i]
                    n = name_arr[i]
                    o = loc_arr[i]
                    data_arr.append([m, n, o, a, b])
        f3 = f"INSERT INTO {self.table_name_equipments}(equipment_id, equipment_name, equipment_location, space_id, project_name) VALUES (%s, %s, %s, %s, %s)"
        cur.executemany(f3, data_arr)
        conn.commit()
        conn.close()

    # added all data to mysql db
    def write_to_db_spaces(self):
        conn = mysql.connector.connect(host="127.0.0.1", user="root", password="toor")
        cur = conn.cursor()
        #
        try:
            f0 = f"CREATE DATABASE IF NOT EXISTS {self.database_name}"
            cur.execute(f0)
        except:
            pass
        #
        f1 = F"USE {self.database_name}"
        cur.execute(f1)
        #
        f2 = f"CREATE TABLE IF NOT EXISTS {self.table_name_spaces} (space_id INTEGER, space_uid VARCHAR(255), \
        space_name VARCHAR(255), space_full_name VARCHAR(255), space_level VARCHAR(255), space_area FLOAT(10, 4),\
        space_bounds VARCHAR(255), space_coordinates TEXT, project_name VARCHAR(255))"
        cur.execute(f2)
        #
        arr = self.df_spaces[
            ['spaceId', 'uniqueIdentifier', 'spaceName', 'spaceFullName', 'level', 'area', 'bounds',
             'coordinates', 'projectName']].values
        data_arr = []
        for i, x in enumerate(arr):
            ar = 0.0
            try:
                ar = float(x[5].split(" ")[0])
            except:
                ar = 0.0

            try:
                a, b, c, d, e, f, g, h, j = x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8]
                data_arr.append([a, b, c, d, e, ar, g, h, j])
            except:
                print(f"error at index {i}, array {arr[i]}")
                pass

        f3 = f"INSERT INTO {self.table_name_spaces}(space_id, space_uid, space_name, space_full_name, space_level, \
        space_area, space_bounds, space_coordinates, project_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cur.executemany(f3, data_arr)
        conn.commit()
        conn.close()
