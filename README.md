
About
==========

A stand-alone project to generate a jar that provides the functionality in the SOLR-4449 patch.


Using
=======

Drop the jar from this project into your solr lib dir.

Configure your solrconfig.xml with a requestHandler that uses a custom shardHandlerFactory, like:

      <requestHandler default="true" name="/backup-enabled" class="solr.SearchHandler">
        <shardHandlerFactory class="HttpBackupRequestShardHandlerFactory">
          <int name="backupRequestDelay">1500</int>
          <int name="maximumConcurrentRequests">2</int>
        </shardHandlerFactory>
      </requestHandler>

In this example, requests to this requestHandler will by default allow up to two concurrent 
requests on each shard, with the second dispatched only after the first has failed to respond 
for 1500ms. The first request to succeed is used, the other is discarded.


Changes since SOLR-4449
=======================

In most respects this is the same code. The test methodology is a bit finicky, (Solr 5.3 is the first official 
release to support the test approach used here) and the amount of code that is copy/pasted from LBHttpSolrClient
due to private qualifiers has changed somewhat. There are also some new features though.
       
Added in the port from Jira is the ability to specify the backupRequestDelay and/or backupRequestDelay on
a per-request basis, like:

    /solr/collection1/backup-enabled?q=*:*&backupRequestDelay=100&maximumConcurrentRequests=3

If not specified in the request, the values configured in the shardHandlerFactory are used.

Also, specifying a "-1" value to either backupRequestDelay or backupRequestDelay disables the 
backup request functionality provided by this jar. This can be used in the shardHandlerFactory config
to disable backup requests by default or per-request, and can be a convenient way to check whether
some odd behaviour or performance is due to backup requests or something else.
 


Author
==========

Philip Hoy originally wrote this patch against Solr 4.4. 
See [SOLR-4449](https://issues.apache.org/jira/browse/SOLR-4449)

Ported to Solr 5.3 and expanded slightly by Jeff Wartes at Whitepages.com

Building
==========

    mvn package
   
This might require Maven 3.x. 

Then put the resulting jar someplace Solr can get it.   
    
Solr versions
=============
    
Built against Solr 5.3
  
