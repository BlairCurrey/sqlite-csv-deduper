# sqlite-csv-deduper
This is a 0 dependency alternative to a hashmap for deduplicating csv files. It only uses modules from the Python standard library. It uses sqlite to enforce uniqueness, which has some interesting trade-offs.

# Why use on-disk sqlite vs. something in-memory?
Advantages:
- Easy garunteed uniqueness using sql unique constraints.
- Bound by hard drive space instead of memory. Good for extremely large datasets that cannot be held in memory.
- Easier to use than implementing an advanced hashing/partitioning strategy that may be required with very large datasets.
- Only uses the standard library. No dependencies.

Drawbacks:
- Not as fast as doing in-memory.

If you can load all of your data into memory, especially if some collisions are acceptable, you should use a hashmap or other in memory data structure. This comes in handy when you have many very large csv files that you cannot load all into memory at once.

For example, you may have ~200 GB of uncompressed csv's and 64GB of memory. Or perhaps you have a smaller 30GB of data on an older computer with 4gb of memory. You may be able to load this all into memory using some variation of hashing, discarding data, or having many duplicates. Or you may not. This can be a good solution for those cases.
 
# Features
- Handles compressed, archived, or regular csv files
- Can enforce deduplicate by a single or multiple columns

# Example usage
Deduplicate by a single column:

    python main.py -i ./data/csvs-with-dupes-gzip -u col1

Deduplicate by multiple columns:
  
    python main.py -i ./data/csvs-with-dupes-gzip -u col1 -col2
