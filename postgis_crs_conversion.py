# 2019 Copyleft Roberto Marzocchi - Gter srl Innovazione in Geomatica Gnss e Gis
import os,sys,shutil,re,glob, getopt
import datetime
#import ogr

#apro connessione PostGIS
import psycopg2

import credenziali
#file credenziali.py contain the following line
#conn = psycopg2.connect(dbname='name', port=5432, user='username', password='pwd', host='XXX.XXX.XXX.XXX')

conn= credenziali.conn

# Open a cursor to perform database operations
curr = conn.cursor()

conn.autocommit = True


#creo file di log
f1=open('./log_cambio_CRS.txt', 'w')
ora=datetime.datetime.now()
ora_file=ora.strftime("%Y%m%d_%H%M%S")
nomefile='{0}_backup_viste_{1}.sql'.format(ora_file,credenziali.nome_db)
f2=open(nomefile, 'w')




f1.write("################# Ricerca tabelle da aggiornare #################")



#select
sql: str = 'SELECT s.nspname,p.relname,t.view_definition,p.relfilenode ' \
		   	'FROM pg_class p ' \
	  		'JOIN pg_catalog.pg_namespace s ON s.oid=p.relnamespace ' \
		   	'JOIN information_schema.views t ON (t.table_name=p.relname AND t.table_schema=s.nspname) ' \
		   	'where table_schema not in (\'gdo\', \'information_schema\', \'pg_catalog\', \'cron\',\'public\') ' \
 			'order by p.relfilenode;'

print(sql)
curr.execute(sql)

for result in curr:
	schema=result[0]
	table=result[1]
	definition=result[2]
	#geometry_column=result[3]
	# srid = result[5]
	# if (result[4]==2) :
	# 	geometry_type=result[6]
	# else:
	# 	geometry_type='{0}Z'.format(result[6])

	f2.write('CREATE OR REPLACE VIEW {0}.{1} AS\n {2}'.format(schema,table,definition))
	#sql2='select definition from pg_views where schemaname=\'{0}\' and viewname = \'{1}\''.format(schema,table)
	#curr2 = conn.cursor()
	#curr2.execute(sql2)
	#for result2 in curr2:
	#	f2.write(result2[0])
	#print(result2[0])
	f2.write('\n\n-- **************************************************\n\n')

curr.close


# nuova query per fare il drop, questa volta l'ordine è inverso
curr = conn.cursor()
#select
# sql: str = 'SELECT t.table_schema, t.table_name, p.relfilenode ' \
# 	  'FROM information_schema.views t, pg_class p ' \
# 	  'where table_schema not in (\'gdo\', \'information_schema\', \'pg_catalog\', \'cron\',\'public\') ' \
# 	  'and p.relname=t.table_name ' \
# 	  'order by p.relfilenode DESC;'
sql: str = 'SELECT s.nspname,p.relname,p.relfilenode,t.view_definition ' \
		   	'FROM pg_class p ' \
	  		'JOIN pg_catalog.pg_namespace s ON s.oid=p.relnamespace ' \
		   	'JOIN information_schema.views t ON (t.table_name=p.relname AND t.table_schema=s.nspname) ' \
		   	'where table_schema not in (\'gdo\', \'information_schema\', \'pg_catalog\', \'cron\',\'public\',\'polizia_locala\') ' \
 			'order by p.relfilenode DESC;'

print(sql)
curr.execute(sql)

for result in curr:
	schema=result[0]
	table=result[1]
	#drop_sql='DROP VIEW {0}.{1};'.format(schema,table);
	drop_sql = 'DROP VIEW {0}.{1} CASCADE;'.format(schema, table)
	curr2 = conn.cursor()
	try:
		print(drop_sql)
		#curr2.execute(drop_sql)
	except psycopg2.Error as e:
		#message = "VIEW {0}.{1} already dropped".format(schema, table)
		#print(message)
		pass
	print(curr2.statusmessage)
	curr2.close
	print('ok')

curr.close


curr = conn.cursor()

#select
sql: str = 'SELECT t.table_schema, t.table_name, t.table_type, ' \
	  'g.f_geometry_column,g.coord_dimension,g.srid,g.type ' \
	  'FROM information_schema.tables t, ' \
	  'public.geometry_columns g ' \
	  'where t.table_name = g.f_table_name ' \
	  'and t.table_schema = g.f_table_schema ' \
	  'and (t.table_type ilike \'base table\') and g.srid=3003 --or t.table_type ilike \'foreign_table\') ' \
	  'order by table_type;'

print(sql)
curr.execute(sql)



###########################################################################
# In this example we perform the transformation from 300301 and 7795
# where the 300301 code has been prevously defined from user
# it is similar to 3003 code but use the ntv2 grid file for the region of interest

#select proj4text from spatial_ref_sys where srid=3003
# +proj=tmerc +lat_0=0 +lon_0=9 +k=0.9996 +x_0=1500000 +y_0=0 +ellps=intl +towgs84=-104.1,-49.1,-9.9,0.971,-2.917,0.714,-11.68 +units=m +no_defs

#select proj4text from spatial_ref_sys where srid=300301
# +proj=tmerc +lat_0=0 +lon_0=9 +k=0.9996 +x_0=1500000 +y_0=0 +ellps=intl +towgs84=-104.1,-49.1,-9.9,0.971,-2.917,0.714,-11.68 +units=m +no_defs  +nadgrids=44301020_46501320_R40_F00.gsb
############################################################################



for result in curr:
	schema=result[0]
	table=result[1]
	type=result[2]
	geometry_column=result[3]
	srid = result[5]
	if (result[4]==2) :
		geometry_type=result[6]
	else:
		geometry_type='{0}Z'.format(result[6])

	sql_alter='ALTER TABLE "{0}"."{1}" ' \
			  'ALTER COLUMN "{2}" TYPE geometry({3}, 7795) ' \
			  'USING st_transform(st_setsrid("{2}", 300301), 7795);'.format(schema,table,geometry_column,geometry_type)
	print(sql_alter)
	curr2 = conn.cursor()
	#decomment thi line to perform the conversion
	#curr2.execute(sql_alter)
	curr2.close




# Close communication with the database
curr.close()
conn.close()


