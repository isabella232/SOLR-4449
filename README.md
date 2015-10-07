
About
==========

A stand-alone project to generate a jar that provides the functionality in the SOLR-4449 patch.


Using
=======

Drop the jar from this project into "server/solr-webapp/webapp/WEB-INF/lib". Due to protected-access modifiers, 
this jar must be loaded by the same classloader as LBHttpSolrClient. 
Also insure [metrics-core](http://search.maven.org/#artifactdetails%7Cio.dropwizard.metrics%7Cmetrics-core%7C3.1.2%7Cbundle)
is available.

Configure your solrconfig.xml with a requestHandler that uses a custom shardHandlerFactory, like:

      <requestHandler default="true" name="/backup-enabled" class="solr.SearchHandler">
        <shardHandlerFactory class="HttpBackupRequestShardHandlerFactory"/>
      </requestHandler>


Fixed-delay backup requests
-----------------------------

Backup request configuration can happen in the shardHandlerFactory definition, like:

      <requestHandler default="true" name="/backup-enabled" class="solr.SearchHandler">
        <shardHandlerFactory class="HttpBackupRequestShardHandlerFactory">
          <int name="backupRequestDelay">1500</int>
          <int name="maximumConcurrentRequests">2</int>
        </shardHandlerFactory>
      </requestHandler>

In this example, requests to this requestHandler will by default allow up to two concurrent 
requests on each shard, with the second dispatched only after the first has failed to respond 
for 1500ms. The first request to succeed is used, the other is discarded.

The delay and permissible concurrent requests can also be given in the query, and will override
the defaults defined in the shardHandlerFactory, like:

    /solr/collection1/backup-enabled?q=*:*&backupRequestDelay=100&maximumConcurrentRequests=3

Performance-aware backup requests
---------------------------------

If you'd prefer not to give an absolute backupRequestDelay value, and instead rely on actual
real-time performance, you can configure latency percentile using the following parameters:

* performanceClass - The name of the type of query to track latency for. Queries with different performance profiles
can be distinguished according to this parameter. If not specified, this defaults to the query path, which is often 
the request handler name.
* backupRequestPercentile - The latency percentile (for this performanceClass) to wait before issuing a backup 
request. One of: NONE, P50, P75, P90, P95, P98, P99, P999.

These can be provided default values in the shardHandlerFactory declaration, and/or specified at query time. For example:

    # Only do the backup request on a "HardQuery" if it takes longer than 99 out of every 100 "HardQuery" requests.
    /solr/collection1/backup-enabled?q=*:*&performanceClass=HardQuery&backupRequestPercentile=P99
    # Do the backup request on an "EasyQuery" if it takes longer than 75 out of every 100 "EasyQuery" requests. 
    /solr/collection1/backup-enabled?q=foo:bar&performanceClass=EasyQuery&backupRequestPercentile=P75

The maximumConcurrentRequests parameter is respected for both the fixed-delay and performance-aware options.

A performanceClass with fewer than 1 request every 10 seconds is assumed to have insufficent data to get meaningful 
performance numbers from, and performance-aware backup requests will not be issued until the rate gets to that level.
Remember that this is measuring post-fan-out distributed-request rate though, not the client query rate.


Other notes
------------

A backupRequestDelay specified in the query overrides anything else. Otherwise, the backupRequestPercentile will
be preferred, if one can be found in either the query or the defaults.

Performance data is tracked in the a SharedMetricsRegistry whose name can be specified in the shardHandlerFactory 
declaration. 
This can be attached to a new [reporter](https://dropwizard.github.io/metrics/3.1.0/manual/core/#reporters) (or an existing one) for metrics export.

To avoid excessive percentile calculations, performance data is cached and updated (at most) every 15 seconds.

The default shardHandlerFactory parameter values enable backup request support, but only explicitly, at query time.



Changes since SOLR-4449
=======================

In most respects this is the same code as the original patch. The test methodology is a bit finicky, (Solr 5.3 is the 
first official release to support the test approach used here) and the amount of code that is copy/pasted from 
LBHttpSolrClient due to private qualifiers has changed somewhat. There are also some new features though.
       
* The ability to specify the backupRequestDelay and/or backupRequestDelay on a per-request basis.
* The ability to disable backup requests and bypass most of the custom code here
* Performance-aware backup requests

The git tag "5.3_port_complete" can be used if you don't want the metrics-core dependency, and only 
need fixed backup delays. 

Author
==========

Philip Hoy originally wrote this patch against Solr 4.4. 
See [SOLR-4449](https://issues.apache.org/jira/browse/SOLR-4449)

Ported to Solr 5.3 and expanded by Jeff Wartes at Whitepages.com

Building
==========

    mvn package
   
This might require Maven 3.x. 

Then put the resulting jar someplace Solr can get it.   
    
Solr versions
=============
    
Built against Solr 5.3
  
