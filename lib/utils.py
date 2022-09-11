import sys
import getopt
import glob
import os
import csv
from sqlite3 import IntegrityError
import gzip
import shutil
from lib.Database import Database

def get_files(in_dir):
    files = glob.glob(in_dir + '/*')

    if len(files) == 0:
      raise Exception(f"No files found in {in_dir}")

    return files

def dedupe_all(in_dir, out_dir, db):
  files = get_files(in_dir)

  for file in files:
    _, ext = os.path.splitext(file)
  
    if ext == '.gz':
      dedupe_gzip(file, out_dir='./data/deduped-gzip', db=db)
    elif ext == '.csv':
      dedupe_file(file, out_dir='./data/deduped-gzip', db=db)
    else:
      dedupe_archive(file, in_dir=in_dir, out_dir='./data/deduped-zip', db=db)

def dedupe_gzip(file, out_dir, db):
  # decompress with gzip
  filename = file.split('.csv')[0] + '.csv'
  with gzip.open(file, 'rb') as f_in:
    with open(filename, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
  
  dedupe_file(filename, out_dir, db)
  
  # remove unompressed csv
  os.remove(filename)

def dedupe_archive(file, in_dir, out_dir, db):
    filename = None

    try:
        shutil.unpack_archive(file, in_dir)
        # remove everything after csv (.gzip, .zip, .hfdgf, etc.)
        filename = file.split('.csv')[0] + '.csv'
    except Exception as e:
        print(e)
        print(f'Error while trying to decompress {file}')

    if filename is not None:
        dedupe_file(filename, out_dir, db)
        os.remove(filename)

def dedupe_file(file, out_dir, db):
    with open(file) as csv_file:
        print(f'Deduping {csv_file.name}')
        csv_reader = csv.reader(csv_file, delimiter=',')
        # skip csv header
        next(csv_reader)

        line_count = 0
        duplicates = 0

        for row in csv_reader:
            line_count += 1
            try:
                # try saving in Deduped table. save into new csv unless a unique constraint exception is raised
                db.cur.execute(f"INSERT INTO {Database.table_name} (name, payload) VALUES (?, ?)", (row[0], row[1]))
                db.con.commit()
                # TODO: save into new csv. or do this after all
                
            except IntegrityError:
                # duplicate - dont save
                duplicates += 1

        print(f'Lines: {line_count}')
        print(f'Duplicates: {duplicates}')

def parse_args(argv):
    in_dir = None
    out_dir = "./deduped"
    unique = []
    arg_help = "{0} -i <in-dir> -o <out-dir> -u <unique>".format(argv[0])
    
    try:
        opts, args = getopt.getopt(argv[1:], "h:i:o:u:", ["help", "in-dir=", "out-dir=", "unique="])
    except getopt.GetoptError as e:
        print(e)
        print(arg_help)
        sys.exit(2)
    
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(arg_help)
            sys.exit(2)
        elif opt in ("-i", "--in-dir"):
            in_dir = arg
        elif opt in ("-o", "--out-dir"):
            out_dir = arg
        elif opt in ("-u", "--unique"):
            unique.append(arg)

    if in_dir is None:
        raise Exception('Must provide an in directory with -i')
    if len(unique) == 0: # empty list
        raise Exception('Must provide a column name to dedupe on with -u')

    return in_dir, out_dir, unique