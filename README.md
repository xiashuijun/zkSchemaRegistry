zkSchemaRegistry
================

A zookeeper registry which allows applications to keep track of which schemas other applications are running.

This is very helpful if you want to namespace memcached [https://github.com/vallancelee/hibernate-memcached] with a schema such that you can avoid memcached poisoning.  The basic overview is that when an application starts up it creates a cache of all other schemas for all applications running.  When all other currently running applicaitons are notified that an application with a new schema is about to start, then then only you can start.  

Basic API

When starting an application 
call SchemaRegistryCache.start(schemaVersion, processIdentifer, ttl)

now you will register with zookeeper, this class also contains the cache where you can check active running schemas at anytime using Set<SchemaVersion> getActiveSchemas() 

There is a listener setup to watch for new ZNodes and automatically update your cache.

The basic design of the ZNode directory structure is as follows

- schema
    - [someSchemaVersion]
        - processA
        - processB

- process
    - processA : data {someSchemaVersion,someOtherSchemaVersion}
    - processB : data {someSchemaVersion,someOtherSchemaVersion}
    - processC : data {someSchemaVersion,someOtherSchemaVersion}  

All nodes are ephermeral except for the schema version nodes.  Those are cleaned up by a reaper.  The reason for this is if a server restarts or you take downtime you do not necessarily want the caches to clear immediately for all other nodes.
That is why you pass the ttl into the start method of SchemaRegistryCache.  When a particular schemaVersion has no children (all the child ephemeral nodes are gone), a ttl is written as the data for that zNode and the reaper will check to see if there have been no children for that znode and the ttl has passed to remove that schema zNode and all caches will update.
