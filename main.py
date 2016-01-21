from math import sqrt, pow
import random
from time import time
import psycopg2
from memory_profiler import memory_usage

from geopoint import Geopoint

""" Max avaliable values """
LAT_MAX = 90
LON_MAX = 180
R_MAX = 40000   # maximum radius is size of equator in kilometers

POINT_LIST = []
POINT_LIST_CLASS = []


def distance(lat1, lon1, lat2, lon2):
    return sqrt(
            pow(lat1 - lat2, 2) + pow(lon1 - lon2, 2)
    )


def value_generator(name):
    multi_and_div = 1000000     # used to obtain 6-decimal-places float values

    if 'lat' == name:
        return random.randrange(-LAT_MAX * multi_and_div, LAT_MAX * multi_and_div) / multi_and_div
    if 'lon' == name:
        return random.randrange(-LON_MAX * multi_and_div, LON_MAX * multi_and_div) / multi_and_div
    if 'r' == name:
        return random.randrange(0, R_MAX * (multi_and_div/100)) / multi_and_div


def generator(number_of_points):
    point_list = []

    for i in range(number_of_points):
        point_list.append([i, value_generator('lat'), value_generator('lon'), value_generator('r')])

    return point_list


def batch_create(point_list):
    point_list_class = []
    for one_point in point_list:
        point_list_class.append(Geopoint(one_point[0], one_point[1], one_point[2], one_point[3]))

    return point_list_class


def list_search(lat, lon, point_list):
    id_list = []

    curtime = time()
    for point in point_list:
        if distance(lat, lon, point[1], point[2]) <= point[3]:
            id_list.append(point[0])

    final_time = time()-curtime

    return id_list, final_time


def class_search(lat, lon, point_list):
    id_list = []

    """ Insert into class """
    point_list_class = batch_create(point_list)

    """ Search """
    curtime = time()
    for point in point_list_class:
        if distance(lat, lon, point.lat, point.lon) <= point.r:
            id_list.append(point.id_number)

    final_time = time()-curtime

    return id_list, final_time


def dict_search(lat, lon, point_list):
    id_list = []

    """ Insert into dictionary"""
    list_dict = []
    for point in point_list:
        temp_dict = {'id' : point[0], 'lat' : point[1], 'lon' : point[2], 'r' : point[3]}
        list_dict.append(temp_dict)

    """ Search """
    curtime = time()
    for point in list_dict:
        if distance(lat, lon, point['lat'], point['lon']) <= point['r']:
            id_list.append(point['id'])

    final_time = time()-curtime

    return id_list, final_time


def database_search(lat, lon, point_list):
    try:
        conn = psycopg2.connect('host=192.168.0.16 user=pgwojtek dbname=cctest password=cctestpasswd0%0')
    except:
        print('Cannot connect to database')
        exit(1)

    cur = conn.cursor()

    """ Insert into database """
    cur.execute('drop table if exists points cascade;')
    conn.commit()

    cur.execute("""create table points (
                    id serial primary key,
                    lat float,
                    lon float,
                    r float
                );"""
                )
    conn.commit()

    for point in point_list:
        cur.execute("""insert into points (lat, lon, r) values(%s, %s, %s)""", (point[1], point[2], point[3]))
    conn.commit()

    """ Search """
    curtime = time()
    cur.execute("""select id from points where |/((%s - lat)^2 + (%s - lon)^2) <= r;""", (lat, lon))
    conn.commit()
    id_list = cur.fetchall()

    final_time =  time()-curtime

    cur.close()
    conn.close()

    return id_list, final_time


def query(lat, lon):
    if lat > LAT_MAX or lat < -LAT_MAX:
        print('Latiture value must be between', -LAT_MAX, 'and', LAT_MAX, '. Your value:', lat)
        return

    if lon > LON_MAX or lon < -LON_MAX:
        print('Longitude value must be between', -LON_MAX, 'and', LON_MAX, '. Your value:', lon)
        return

    no_of_records_base = 100
    for no_of_records in [no_of_records_base, no_of_records_base*10, no_of_records_base*100]:
        print('Checking times for', no_of_records, 'number of records')

        point_list = generator(no_of_records)

        """ List-based searching """
        search_name =  'list'
        print(memory_usage((list_search, (lat, lon, point_list))))
        id_list, final_time = list_search(lat, lon, point_list)
        time_dict = {search_name : final_time}
        index_dict = {search_name : id_list}


        """ Class-based searching """
        search_name =  'class'
        print(memory_usage((class_search, (lat, lon, point_list))))
        id_list, final_time = class_search(lat, lon, point_list)
        time_dict[search_name] = final_time
        index_dict[search_name] =  id_list


        """ Dictionary searching """
        search_name =  'dict'
        id_list, final_time = dict_search(lat, lon, point_list)
        print(memory_usage((dict_search, (lat, lon, point_list))))
        time_dict[search_name] = final_time
        index_dict[search_name] =  id_list


        """ Database searching """
        search_name =  'database'
        print(memory_usage((database_search, (lat, lon, point_list))))
        id_list, final_time = database_search(lat, lon, point_list)
        time_dict[search_name] = final_time
        index_dict[search_name] =  id_list

        # print('Times\n', time_dict)
        print(no_of_records)
        for name in time_dict:
            print(time_dict[name])

def main():

    query(58.435287, 100)


if __name__ == '__main__':
    main()