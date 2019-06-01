#!/bin/sh
# version 1.0

#docker container should be running

DATABASE=streampipes
USERNAME=streampipes
PORT=65432s
HOSTNAME=localhost
export PGPASSWORD=streampipes

# basic setup
psql -h $HOSTNAME -U $USERNAME -p $PORT $DATABASE << EOF

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pgrouting;

CREATE SCHEMA IF NOT EXISTS routing AUTHORIZATION streampipes;
CREATE SCHEMA IF NOT EXISTS geofence AUTHORIZATION streampipes;
CREATE SCHEMA IF NOT EXISTS precipitation AUTHORIZATION streampipes;
CREATE SCHEMA IF NOT EXISTS elevation AUTHORIZATION streampipes;

CREATE TABLE geofence.main (
id SERIAL PRIMARY KEY,
time TIMESTAMP,
name TEXT NOT NULL UNIQUE
);

CREATE TABLE geofence.info (
name TEXT NOT NULL UNIQUE,
geom GEOMETRY,
wkt  TEXT,
epsg integer,
area REAL,
areaunit TEXT,
m real,
FOREIGN KEY (name) REFERENCES geofence.main (name) ON UPDATE CASCADE ON DELETE CASCADE );
EOF

exit 0

# todo implement trigger