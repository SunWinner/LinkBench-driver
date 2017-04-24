# LinkBench-driver
Implemented the database driver（Titan ,OrientDB, Neo4j）

Among them, NEO4J GitHub according to the code from the debugging, repair some of the hidden BUG, unable to compile the problem, and some versions of the jar package does not correspond to the problem.
NEO4J I use the embedded database, that is, call the resources of neo4j in the JVM virtual out of a database. To avoid communication between ports. But the performance is not the best, this is an unknown at this time, waiting for the overthrow of future generations.
The OrientDB database is driven by GraphAPI。Which document api implementation, I will be completed in the next time.
Which query, I used four iterators for the two properties of the query, I believe that my code quality is not the best, the query performance can continue to optimize, but for how to optimize, this is not my recent task, I believe that someone else has read my code and sent me advice. Linkbench for small-scale load, will lead to a jump to increase the relationship, this is the load phase need to be considered, because in the increase before the relationship to consider whether the point already exists in the database. First make two judgments, so that the performance of load down a lot.
