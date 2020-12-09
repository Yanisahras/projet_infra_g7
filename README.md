# Projet Infrastructure de données Groupe 7

Le but du projet est de récupèrer de les donnée des diffèrents artistes à partir de différents albums à partir de l'API Spotify et faire plusieurs analyses en spar Scala. 
Monter notre job au plan afin que ce dernier soit lancé en automatique à partir de Airflow.

Faire de la restitution et visualiser nos données une fois traité par Spar Scala sur Kibana.

## Technologies utilisées :
 #### Scala : 2.12.12
 #### Spark : 2.4.4
 #### JDK : 1.8
 #### Python : 3.x
 
 
 ## Visualisation : 
 #### Elastic Search
 #### Kibana   


## Ordonnoncement :

 #### Airflow


## Architecture 

SpotifyAPI ---> App --> csvfiles --> hdfs --> Spark Batch Processing --> news csv --> Restitution with ELK.