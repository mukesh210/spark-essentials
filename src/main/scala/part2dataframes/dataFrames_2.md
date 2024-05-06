DataFrames: Distributed collection of Rows conforming to a Schema

Schema: list describing the column names and types
    - types known to Spark, not at compile time
    - arbitrary number of columns
    - all rows have the same structure

Need to be distributed
    - data too big for a single computer
    - too long to process the entire data on single CPU

Partitioning
    - splits the data into files, distributed between nodes in the cluster
    - impacts the processing parallelism

DataFrames:
    - Immutable: Can't be changes once created. Create other DF via Transformations

Transformations:
    - narrow: One input partition contributes to at most one output partition(e.g. map)
    - wide: input partition(one or more) create many output partition(e.g. sort)
        Shuffle: Data exchange between cluster nodes
        - occur in wide transformations
        - expensive so massive performance topic


Computing DataFrames
Lazy evaluation:
    - Spark waits until the last moment to execute the DF transformations
    
Spark does **LAZY EVALUATION** for computing DataFrames. It does Planning as well.
    - Spark compiles the DF transformations into a graph before running any code
    - logical plan: DF dependency graph + narrow/wide transformations sequence
    - physical plan: Optimized sequence of steps for nodes in the cluster
    - Since Spark delays execution, it is able to optimize queries: by avoiding multiple passes over data, etc.

Transformations vs Actions
    - Transformations describe how new DFs are obtained
    - Actions actually start executing spark code
