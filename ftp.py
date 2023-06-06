from operator import index
import pandas as pd
import numpy as np
from ftplib import *
import os
from datetime import datetime
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from scipy.signal import find_peaks
from peakdetect import peakdetect
import ast 
import sqlalchemy
from apscheduler.schedulers.blocking import BlockingScheduler
import time
import psycopg2
import datetime
import math
from statistics import stdev,mean
from psycopg2.extensions import register_adapter, AsIs

def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
register_adapter(np.float64, addapt_numpy_float64)
register_adapter(np.int64, addapt_numpy_int64)

#Variable globale
e=0

#Définitions variables globale pourcentage
v1=1
v2=1
v3=1
v4=1
v5=1
v6=1

def init():
    print('begin init...')
    time.sleep(5)
    print('end init...')

def transform():
    print('Begin ETL...')
    global e
    global v1,v2,v3,v4,v5,v6
    #Récupération des données du serveur FTP
    global ftp
    ftp = FTP("ftp.oceanbox.net")
    ftp.login("cocorico3", "MmE2MT")
    ftp.cwd("/ship_to_shore/adrena")
   
    filename="journalR.txt"
    with open(filename,"wb") as file:
        ftp.retrbinary(f"RETR {filename}",file.write)
        
    # Display the content of downloaded file
    file= open(filename, "r")
    print('File Content:', file.read())

    #fileDir = os.path.dirname(os.path.realpath('__file__'))
    #print (fileDir)

    # Close the Connection
    ftp.quit()
    
    pol=pd.read_csv('polaire.csv',sep=';',encoding="latin-1")
    polaire=pol.copy()
    c1 = pd.read_csv (r"C:/Users/cantr/Desktop/Stage ingénieur KOMILFO SPORT/Dashboard/journalR.txt",sep=';',encoding='latin-1')
    course=c1.copy()
    
    #TRANSFORMATION DES DONNEES :
    
    #Ajouts des headers
    course.columns=["Date","Position","BSP","SOG","COG","TWS","TWD","TWA","Pression"]

    #Séparation d'une colonne en deux
    c=course.Date.str.split(expand=True)
    course["Date"]=c[0]
    course["Heure"]=c[1]

    course[['Date']] = course[['Date']].applymap(str).applymap(lambda s: "{}/{}/{}".format(s[2:4],s[0:2],s[4:8] ))
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
    #course.to_csv(r"C:/Users/cantr/Desktop/Stage ingénieur KOMILFO SPORT/Dashboard/result2.csv", index=None)
    
    # ENRICHISSEMENT DES DONNEES : 
    
    #Ajout de la colonne %polaire:
    
    course["indexA"]=0
    course["indexS"]=0
     
    #Utiliser la polaire pour avoir une nouvelle colonne %vitesse cible grâce aux angles de vent et aux vitesse de vent réel
    
    for i in course["TWA"].index:
            diffA=abs(polaire["0"]-course["TWA"][i])
            course["indexA"][i]=diffA.argmin()

    for j in course["TWS"].index:
            diffS=abs(polaire.loc[0].astype(float)-course["TWS"][j])
            course["indexS"][j]=diffS.argmin()   
            
    course["polaire"]=0
    for i in course["indexS"].index:
        iS=str(course["indexS"][i])
        iA=course["indexA"][i]
        course["polaire"][i]=(course["SOG"][i]/float(polaire[iS][iA]))*100
        
    course.pop("indexA")
    course.pop("indexS")
    course["polaire"]=course["polaire"].astype(float)
    
    #Ajout de la colonne VMG:
    
    course["VMG"]=0
    for i in course["SOG"].index:
        course["VMG"][i]=course["SOG"][i]*math.cos(course["TWA"][i])
        
    #Repérer les virements et empannages via le COG et les afficher sur le dashboard
    #Repérer le nombre de virements et empannages
    #Enregistrer dans une variable globale la dernière valeur du fichier dans le cas ou le virement se fait pile sur la premère valeur 
    #Faire attention à si la durée du virement dure plus lontemps que la durée d'un fichier
    
    dim=course.shape
    print("________________________________")
    print("écart type:",stdev(course["COG"]))
    print("taille liste:",dim[0])
    print("Angle manoeuvre:",abs(mean(course["COG"])-mean(course["TWD"])))
    print("________________________________")
    
    course["Manoeuvre"]="Pas de manoeuvre en cours"
    if(stdev(course["COG"])>50):
        if(abs(mean(course["COG"])-mean(course["TWD"]))>90):
            course["Manoeuvre"][dim[0]-1]="Empannage"        
        course["Manoeuvre"][dim[0]-1]="Virement"
        
    """
    #Repérer les peaks correspondant aux manoeuvres
    peak=peakdetect(course["SOG"],lookahead=20)

    higherPeaks=np.array(peak[0])
    lowerPeaks=np.array(peak[1])

    higherPeaks=higherPeaks[higherPeaks[:,1]>15]
    lowerPeaks=lowerPeaks[lowerPeaks[:,1]<-15]
    
    lower=list(lowerPeaks[:,0])
    higher=list(higherPeaks[:,0])

    Peaks=sorted(lower+higher)
    """
    
    #Répartir les pourcentages de vent rencontrés, et efficacités en fonction du %polaire
    
    if(e==0):
        course["%V1"]=1.0
        course["%V2"]=1.0
        course["%V3"]=1.0
        course["%V4"]=1.0
        course["%V5"]=1.0
        course["%V6"]=1.0
    
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
    
    course.info()
    
    # Mettre la voile théorique utilisé :
    #Machine learning Clustering pour repérer en fonction de l'angle et de la force du vent quelle voile il faut utiliser
    
    
    
    
    
    #Repérer les comportements anormaux dans les données :
    
    
    
    
    
    # NETTOYAGE DES DONNEES
         
    #Nettoyage des colonnes par moyenne mobile ou autres méthodes  si données plus importante,
    #Appliquer les paramètres de nettoyage en fonction de la dispersion des données
    #Plutôt que d'avoir un paramètrage constant :
    
    """
    course["SOG"]=moving_average(course["SOG"],10)
    course["TWS"]=moving_average(course["SOG"],10)
    course["TWA"]=moving_average(course["SOG"],10)  
    """
    
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
        
        h=0
        for row in res:
            h=row[0]
            print("t")
        #format ='%Y-%m-%d %H:%M:%S'
        #course["Horaire"]=course["Horaire"].dt.datetime
        print("course :",course["Horaire"].min())
        print(": ",h)
        #course["Horaire"]=datetime.datetime.strptime(str(course["Horaire"]),format)
        print("Nouveau fichier arrivé :",h<course["Horaire"].min())
        if h<course["Horaire"].min():   
            for i in course.index: 
                    sql = """INSERT INTO OF50(Horaire, Latitude, Longitude, BSP, SOG, COG, TWS, TWD, TWA, Pression, Polaire, VMG, Manoeuvre, V1, V2, V3, V4, V5, V6) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""
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
    print('end ETL...')
    

if __name__ == '__main__':
    scheduler = BlockingScheduler(timezone='Europe/Paris', executors={'default': ThreadPoolExecutor(1)})
    scheduler.add_job(init)
    scheduler.add_job(transform, "interval", seconds=60, misfire_grace_time=30)
    scheduler.print_jobs()
    scheduler.start()

    try:
        while True:
            time.sleep(5)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
