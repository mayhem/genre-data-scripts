#!/usr/bin/env python

import sys
import os
import psycopg2
import json

PG_CONNECT = "dbname=acousticbrainz user=acousticbrainz host=localhost port=5432"

def dump_data(json_file, out_dir):
    try:
        f = open(json_file, "r")
        data = f.read()
        f.close()
    except IOError as e:
        print "Failed to read data to JSON file: %s" % e

    genres = json.loads(data)

    print "\nSummary:"
    for genre in genres:
        print "%-20s: %d" % (genre, len(genres[genre]))

    conn = psycopg2.connect(PG_CONNECT)
    cur = conn.cursor()

    data = {}
    for genre in genres:
        print "Process genre '%s':" % genre
        cur.execute("""SELECT mbid, data 
                         FROM lowlevel
                        WHERE mbid in %s""", (tuple(genres[genre]),))
        while True:
            row = cur.fetchone()
            if not row:
                break

            mbid = row[0]
            ll_data = row[1]

            if mbid not in data:
                data[mbid] = ll_data
            else:
                if not data[mbid]["metadata"]["audio_properties"]["lossless"] and \
                   ll_data["metadata"]["audio_properties"]["lossless"]:
                    data[mbid] = ll_data

        print "  found %d of %d mbids. Writing to disk." % (len(data), len(genres[genre]))
        if not os.path.exists(os.path.join(out_dir, genre)):
            os.mkdir(os.path.join(out_dir, genre))
        for mbid in data:
            try:
                f = open(os.path.join(out_dir, genre, mbid + ".json"), "w")
                f.write(json.dumps(data[mbid]))
                f.close()
            except IOError as e:
                print "Cannot write file to output dir: %s" % e
                sys.exit(-1)

        try:
            f = open(os.path.join(out_dir, genre, "MANIFEST"), "w")
            for mbid in sorted(data):
                f.write(mbid + "\n")
            f.close()
        except IOError as e:
            print "Cannot write MANIFEST file to output dir: %s" % e
            sys.exit(-1)

        data = {}
        print "  done"

if __name__ == "__main__":
    # Hi alastair!
    if len(sys.argv) < 3:
        print "Usage: %s <genre json file> <output dir>"
        sys.exit(-1)

    json_file = sys.argv[1]
    out_dir = sys.argv[2]

    if not os.path.exists(out_dir):
        print "output directory %s does not exist." % out_dir
        sys.exit(-1)

    if not os.path.exists(json_file):
        print "input file %s does not exist." % json_file
        sys.exit(-1)

    dump_data(json_file, out_dir)
