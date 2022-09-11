import sys
import time
from lib.Database import Database
from lib.utils import dedupe_all, parse_args

def main():
  start = time.time()
  in_dir, out_dir, unique = parse_args(sys.argv)

  # inits and connects to db
  db = Database(unique)

  # show empty table
  db.print_info()

  # deduped csvs
  dedupe_all(in_dir=in_dir, out_dir=out_dir, db=db)

  # Show some table info
  db.print_info()
  
  # TODO: save table to csv?
  
  # close database connection
  db.con.close()

  end = time.time()

  print(f'Deduplications time: {"{:04.2f}".format(end - start)} seconds')

if __name__ == '__main__':
  main()