import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import numpy as np
from scipy.signal import find_peaks
from peakdetect import peakdetect
from ftplib import *
import os
import time
from datetime import datetime
import ast 
import psycopg2
#import wget 
from psycopg2.extensions import register_adapter, AsIs


#Définitions variables globale pourcentage
e=0
v1=1
v2=1
v3=1
v4=1
v5=1
v6=1

def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
register_adapter(np.float64, addapt_numpy_float64)
register_adapter(np.int64, addapt_numpy_int64)
            
def filterdata(df,hour_selected):
    return df[df["DateTime"].dt.hour==hour_selected]

st.set_page_config(page_title="Dashboard",
                   page_icon="bar_chart:",
                   layout="wide"
)


pol=pd.read_csv('polaire.csv',sep=';',encoding="latin-1")
polaire=pol.copy()
c1 = pd.read_csv (r"C:/Users/cantr/Desktop/Stage ingénieur KOMILFO SPORT/Dashboard/data-ecup.txt",sep=';',encoding='latin-1')
course=c1.copy()

#Ajouts des headers
course.columns=["Date","Position","BSP","SOG","COG","TWS","TWD","TWA","Pression"]

#Séparation d'une colonne en deux
c=course.Date.str.split(expand=True)
course["Date"]=c[0]
course["Heure"]=c[1]

course[['Date']] = course[['Date']].applymap(str).applymap(lambda s: "{}/{}/{}".format(s[0:2],s[2:4],s[4:8] ))
course[['Heure']] = course[['Heure']].applymap(str).applymap(lambda s: "{}:{}:{}".format(s[0:2],s[2:4],s[4:8] ))

#Convertir en coordonnées GPS
p=course.Position.str.split(",",expand=True)

for i in p[0].index:
    p[0][i]=ast.literal_eval(p[0][i][:2])+ast.literal_eval(p[0][i][2:])/60
    if (p[1][i]=='S'):
        p[0][i]=-p[0][i]
        
course["Latitude"]=round(p[0].astype(float),3)

for i in p[2].index:
    p[2][i]=int(p[2][i][:3])+ast.literal_eval(p[2][i][3:])/60
    if (p[3][i]=='W'):
        p[2][i]=-p[2][i]
              
course["Longitude"]=round(p[2].astype(float),3)    
course.pop("Position")
course["Horaire"]=pd.to_datetime(course.pop('Date'))+pd.to_timedelta(course.pop('Heure'))
course=course[['Horaire',"Latitude","Longitude","BSP","SOG","COG","TWS","TWD","TWA","Pression"]]
course.info()
course.to_csv(r"C:/Users/cantr/Desktop/Stage ingénieur KOMILFO SPORT/Dashboard/result4.csv", index=None)

course["COG"]=course["COG"].astype(float)
course["TWD"]=course["TWD"].astype(float)
course["TWA"]=course["TWA"].astype(float)

#Ajout de la colonne %polaire:
course["indexA"]=0
course["indexS"]=0

#Utiliser la polaire pour avoir une nouvelle colonne %vitesse cible grâce aux angles de vent et aux vitesse de vent réel
#course=course.reset_index()
for i in course["TWA"].index:
        diffA=np.absolute(polaire["0"]-course["TWA"][i])
        course["indexA"][i]=diffA.argmin()

for j in course["TWS"].index:
        diffS=np.absolute(polaire.loc[0].astype(float)-course["TWS"][j])
        course["indexS"][j]=diffS.argmin()   
            
course["polaire"]=0
for i in course["indexS"].index:
    iS=str(course["indexS"][i])
    iA=course["indexA"][i]
    course["polaire"][i]=(course["SOG"][i]/float(polaire[iS][iA]))*100
    
course.pop("indexA")
course.pop("indexS")

course["polaire"]=course["polaire"].astype(float)
print(type(course["polaire"]))
print(course["polaire"].values.astype(float))  
print(course["TWS"])  

#Repérer les virements et empannages via le SOG et les afficher sur le dashboard

#Repérer les peaks correspondant aux manoeuvres
"""
peak=peakdetect(course["SOG"],lookahead=20)

higherPeaks=np.array(peak[0])
lowerPeaks=np.array(peak[1])

higherPeaks=higherPeaks[higherPeaks[:,1]>15]
lowerPeaks=lowerPeaks[lowerPeaks[:,1]<-15]
    
lower=list(lowerPeaks[:,0])
higher=list(higherPeaks[:,0])

Peaks=sorted(lower+higher)
"""
if(e==0):
    course["%V1"]=1
    course["%V2"]=1
    course["%V3"]=1
    course["%V4"]=1
    course["%V5"]=1
    course["%V6"]=1
    
vtot=v1+v2+v3+v4+v5+v6
for i in course["TWS"].index:
    if (0<=course["TWS"][i]<5):
        v1=v1+1
        course["%V1"]=(v1/vtot)*100
    if (5<=course["TWS"][i]<10):
        v2=v2+1
        course["%V2"]=(v2/vtot)*100
    if (10<=course["TWS"][i]<15):
        v3=v3+1
        course["%V3"]=(v3/vtot)*100
    if (15<=course["TWS"][i]<20):
        v4=v4+1
        course["%V4"]=(v4/vtot)*100
    if (20<=course["TWS"][i]<25):
        v5=v5+1
        course["%V5"]=(v5/vtot)*100
    if (25<=course["TWS"][i]):
        v6=v6+1
        course["%V6"]=(v6/vtot)*100  
        
try:
    conn = psycopg2.connect(
        user = "postgres",
        password = "Frenzy15!,",
        host = "localhost",
        port = "5432",
        database = "Cocorico"
    )
    
    cur = conn.cursor()
    sql = "SELECT horaire FROM OF50 WHERE id = (SELECT MAX(id) FROM OF50)"
    cur.execute(sql)
    res = cur.fetchall()
    print("type:",type(course["polaire"]))
    
    h=0
    for row in res:
        h=row[0]
        print("t")
    print("course :",course["Horaire"].min())
    
    sql="SELECT tws FROM OF50"
    cur.execute(sql)
    res = cur.fetchall()
    
    print("longueur de liste:",len(res))
    """
    tws=[]
    for i in range(0,len(res)-1):
        for row in res:
            tws[i]=row[0]
    print("tws:",tws)
    """
    #print(type(h))  
    for i in course.index:
            sql = """INSERT INTO OF50(Horaire, Latitude, Longitude, BSP, SOG, COG, TWS, TWD, TWA, Pression, Polaire, V1, V2, V3, V4, V5, V6) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
            value = list(course.loc[i])
            cur.execute(sql, value)
            conn.commit()
            count = cur.rowcount
    print (count, "enregistrement inséré avec succès dans la table OF50.")
        
    #fermeture de la connexion à la base de données
    cur.close()
    conn.close()
    print("La connexion PostgreSQL est fermée")
        
except (Exception, psycopg2.Error) as error :
    print ("Erreur lors de l'insertion dans la table OF50", error)
    
#course["Latitude"]=[lat[:2]+"°"+lat[2:] for lat in course["Latitude"]]

#course.set_index('DateTime',inplace=True)
fig_map=px.line_mapbox(
    course,
    lat="Latitude",
    lon="Longitude",
    zoom=20,
    width=1500,
    height=600
)

fig_map.update_layout(mapbox_style="open-street-map",
                      mapbox_layers=[
        {
            "below": 'traces',
            "sourcetype": "raster",
            "sourceattribution": "United States Geological Survey",
            "source": [
                "https://basemap.nationalmap.gov/arcgis/rest/services/USGSImageryOnly/MapServer/tile/{z}/{y}/{x}"
            ]
        },
        {
            "sourcetype": "raster",
            "sourceattribution": "Government of Canada",
            "source": ["https://geo.weather.gc.ca/geomet/?"
                       "SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&BBOX={bbox-epsg-3857}&CRS=EPSG:3857"
                       "&WIDTH=1000&HEIGHT=1000&LAYERS=RADAR_1KM_RDBR&TILED=true&FORMAT=image/png"],
        }
      ],
                      mapbox_zoom=4,
                      mapbox_center_lat=48,
                      margin={"r":0,"t":0,"l":0,"b":0}
)

fig_wind_speed=go.Figure()
fig_wind_speed.add_trace(go.Scatter(
    x=course["Horaire"],
    y=course["TWS"],
    mode='lines',
    line=dict(color='blue',width=5)
))

fig_wind_speed.update_layout(
    title=dict(text="TWS",
               font=dict(family='Arial',
                         size=30)),
    plot_bgcolor="rgba(0,0,0,0)",
    xaxis=(dict(showline=True,
                showgrid=False,
                showticklabels=True,
                linewidth=2,
                ticks='outside')),
    yaxis=(dict(showgrid=False,
                zeroline=False,
                showline=False,)
           )
)
st.plotly_chart(fig_map)
st.plotly_chart(fig_wind_speed)

