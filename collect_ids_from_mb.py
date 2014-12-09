#!/usr/bin/env python

import sys
import os
import psycopg2
import json

PG_CONNECT = "dbname=musicbrainz user=musicbrainz host=localhost port=5432"

cluster_name = ""
tags = []
clusters = {}
lines = sys.stdin.readlines()
lines.append("\n")

for line in lines:
    line = line.strip()
    if not line and cluster_name:
        clusters[cluster_name] = tags
        tags = []
        cluster_name = ""
        continue
        
    if not cluster_name:
        cluster_name = line

    tags.append(line)

genres = {}
for cluster in clusters:
    print "Processing cluster '%s':" % cluster
    for tag in clusters[cluster]:
        print "  %s" % tag
        conn = psycopg2.connect(PG_CONNECT)
        cur = conn.cursor()
        cur.execute("""SELECT gid FROM musicbrainz.recording r, musicbrainz.recording_tag rt, musicbrainz.tag t 
                        WHERE rt.tag = t.id 
                          AND t.name = %s
                          AND r.id = rt.recording""", (tag,))
        if cluster not in genres:
            genres[cluster] = []

        for row in cur.fetchall():
            genres[cluster].append(row[0])

print "\nSummary:"
for genre in genres:
    print "%-20s: %d" % (genre, len(genres[genre]))

try:
    f = open("genre_mbids.json", "w")
    f.write(json.dumps(genres))
    f.close()
except IOError as e:
    print "Failed to write data to JSON file: %s" % e
