<h2 align="center">Data Architecture</h3>

## ⚙️ Setup du serveur FTP / Envoi des fichiers :
- Configurer l'envoi des données par le biais d'Adrena
- Vérifier l'envoi et la fréquence par le biais de WinSCP


## 📝 Mise en place d'une pipeline ETL :
- Se connecter au serveur FTP avec la bibliothéque ftplib 
- Nettoyer et transformer les données avec les bibliothéques Pandas,Numpy
- Enrichir en calculant de nouvelles données avec le fichier polaire.csv ou avec d'autres données
- Stocker les données dans la BDD (PostgreSQL) avec la bibliothéque psycopg2


## 📩 Stockage des données :
- Créer la BDD en installant postgresql et en lancant ensuite le requete.sql
- Vérifier la création de la table avec pgAdmin

## ⚙️ Configuration de grafana :
- Installer grafana 
- Connecter la BDD postgresql dans les sources et désactiver le mode TLS/SSL
- Utiliser les panneaux et réaliser différentes requêtes SQL pour récupérer les informations utiles


<p align="center">
  <img src="Screen page grafana/grafana route du rhum.JPG"/>
</p>
