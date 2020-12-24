import psycopg2
import time
import datetime

conn = psycopg2.connect("dbname=homework3 user=postgres password=p")
cur = conn.cursor()

time_format = '%Y-%m-%d %H:%M:%S'
#prima di utilizzare dei dati assicurarsi che abbiano il crs 3857
#create table passeggiataollie3857 as (select track_seg_point_id, time,st_transform(geom,3857) from passeggiataollie)

def getDistance(geom1,geom2):
    cur.execute("select st_distance(%s,%s)",(geom1,geom2))
    return cur.fetchone()[0]
def getLat(geom):
    cur.execute("select st_y(%s)",[geom])
    return cur.fetchone()[0]
def getLong(geom):
    cur.execute("select st_x(%s)",[geom])
    return cur.fetchone()[0]
def getEpoch(timestamp):
    cur.execute("select extract(epoch from timestamp %s)",[str(timestamp)])
    return cur.fetchone()[0]
def getTimestamp(epoch):
    cur.execute("select to_timestamp(%s)",[epoch])
    return cur.fetchone()[0]
def getGeom(coordinates):
    cur.execute("select st_geomfromtext('Point (%s %s)')",coordinates)
    return cur.fetchone()[0]
def insertStaypoint(values):
    cur.execute("INSERT INTO AAA VALUES(default, %s, %s, %s)",values)
    conn.commit()
def computMeanCoord(gpsPoints):
    long = 0.0
    lat = 0.0
    for point in gpsPoints:
        long += float(getLong((point[2])))
        lat += float(getLat((point[2])))
    return (long/len(gpsPoints), lat/len(gpsPoints))


def stayPoint( distThres = 10, timeThres = 120):#i risultati migliori li ottengo con 10 metri e 90 secondi
  
    cur.execute('select * from passeggiataolliefinal')
    points = cur.fetchall()
    cur.execute("DROP TABLE aaa;")
    cur.execute("CREATE TABLE AAA (id serial PRIMARY KEY,geome geometry, arriveTime time, leaveTime time);")
    conn.commit()
    pointNum = len(points)
    i = 0
    while i < pointNum-1: 
        j = i+1
        while j < pointNum:
        
            #calcolo la distanza tra i punti pi e pj
            
            pi = points[i]
            pj = points[j]
            
            dist = getDistance(pi[2],pj[2])


            if dist > distThres:
                #se la distanza appena calcolata e' maggiore di distTres calcolo la differenza di tempo tra i due punti

                t_i=getEpoch(pi[1])
                t_j=getEpoch(pj[1])
                #t_i = time.mktime(datetime.datetime.strptime(str(pi[1]),time_format).timetuple())
                #t_j = time.mktime(datetime.datetime.strptime(str(pj[1]),time_format).timetuple())
                deltaT = t_j - t_i
                
                if deltaT > timeThres:
                    #se la differenza di tempo e' maggiore del treshold del tempo devo aggiungere il punto appena trovato
                    coords = computMeanCoord(points[i:j+1])#coords e' una tupla cosi' (lat,long)
                    geom = getGeom(coords)
                    arriveTime=getTimestamp(t_i)
                    leaveTime=getTimestamp(t_j)
                    
                    insertStaypoint((geom, arriveTime, leaveTime))
                    
                break
            j += 1
        i = j
    cur.close()
    conn.close()    

stayPoint()