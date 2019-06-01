#!/bin/sh
# version 1.0

echo starting downloading osm

#execute first database_setup 
# you can download any osm file from geofabrik. Bawü import takes around 5 hrs! execute first database_setup 

wget https://download.geofabrik.de/europe/germany/baden-wuerttemberg-latest.osm.pbf

osmconvert -verbose baden-wuerttemberg-latest.osm.pbf --drop-author --drop-version --out-osm -o=baden-wuerttemberg-latest.osm
osm2pgrouting --f baden-wuerttemberg-latest.osm --clean --conf /usr/share/osm2pgrouting/mapconfig.xml --dbname streampipes -W streampipes --host localhost --port 65432 --username streampipes --schema routing  --no-index 


echo IMPORT FINISHED

# basic setup
psql -h $HOSTNAME -U $USERNAME -p $PORT $DATABASE << EOF

CREATE INDEX IF NOT EXISTS pgr_id_index
ON routing.ways_vertices_pgr (id);
  
CREATE INDEX IF NOT EXISTS pgr_osm_id_index
ON routing.ways_vertices_pgr(osm_id);
  
CREATE INDEX IF NOT EXISTS pgr_geom_index
ON routing.ways_vertices_pgr
USING gist(the_geom)

CREATE INDEX IF NOT EXISTS ways_osm_id_index
ON routing.ways(osm_id);

CREATE INDEX IF NOT EXISTS ways_cost_s_index
ON routing.ways(cost_s);

CREATE INDEX IF NOT EXISTS ways_length_m_index
ON routing.ways(length_m);
  
CREATE INDEX IF NOT EXISTS ways_geom_index
ON routing.ways
USING gist(the_geom)


drop table if exists routing.validarea ;
create table routing.validarea AS
select 1 as id, ST_ConvexHull(ST_Collect(the_geom)) As geom from routing.ways;
EOF

echo finished
exit 0
