# postgis_crs_conversion
CRS conversion for a entire PostGIS geoDB using ntv2 grid - (Italian documentation)


Istruzioni script
----------------------------------------------------


Questo script consente di effettuare l'intera trasformazione dei dati di un geoDb da un CRS all'altro. 
Prevede, se necessario, la corretta definizione dei CRS su PostGIS. 

Lo script consta di 3 step: 

* creazione SQL di backup delle eventuali viste
* rimozione eventuali viste
* riproiezione tabelle


Lo script deve essere accoppiato ad un file credenziali.py in cui sono definite 2 variabili:

* nome_db : nome del DB
* conn : parametri di connessione
