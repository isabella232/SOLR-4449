package org.apache.solr.client.solrj;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import org.apache.commons.io.FileUtils;
import org.apache.http.client.HttpClient;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.*;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

/**
 * Test for LBHttpSolrServer
 *
 * @since solr 1.4
 */
@Slow
@ThreadLeakFilters(defaultFilters = true, filters = {
    SolrIgnoredThreadsFilter.class, QuickPatchThreadsFilter.class})
public class TestBackupLBHttpSolrClient extends LuceneTestCase {
  HashMap<String,SolrInstance> solr;
  HttpClient httpClient;
  ThreadPoolExecutor commExecutor;
  private BackupRequestLBHttpSolrClient lbSolrServer;
  // TODO: fix this test to not require FSDirectory
  static String savedFactory;

  @BeforeClass
  public static void beforeClass() {
    savedFactory = System.getProperty("solr.DirectoryFactory");
    System.setProperty("solr.directoryFactory",
        "org.apache.solr.core.MockFSDirectoryFactory");
  }

  @AfterClass
  public static void afterClass() {
    if (savedFactory == null) {
      System.clearProperty("solr.directoryFactory");
    } else {
      System.setProperty("solr.directoryFactory", savedFactory);
    }
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    httpClient = HttpClientUtil.createClient(null);
    HttpClientUtil.setConnectionTimeout(httpClient, 1000);
    solr = new HashMap<String,SolrInstance>();
    commExecutor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 5,
        TimeUnit.SECONDS, // terminate idle threads after 5 sec
        new SynchronousQueue<Runnable>(), // directly hand off tasks
        new DefaultSolrThreadFactory("TestBackupLBHttpSolrServer"));
  }

  private void addDocs(SolrInstance solrInstance) throws IOException, SolrServerException {
    List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
    for (int i = 0; i < 10; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", i);
      doc.addField("name", solrInstance.name);
      docs.add(doc);
    }
    HttpSolrClient solrClient = new HttpSolrClient(solrInstance.getUrl(), httpClient);
    UpdateResponse resp = solrClient.add(docs);
    assertEquals(0, resp.getStatus());
    resp = solrClient.commit();
    assertEquals(0, resp.getStatus());
  }

  @Override
  public void tearDown() throws Exception {
    ExecutorUtil.shutdownNowAndAwaitTermination(commExecutor);
    if (lbSolrServer != null) lbSolrServer.close();
    for (SolrInstance aSolr : solr.values()) {
      aSolr.tearDown();
    }
    httpClient.getConnectionManager().shutdown();
    super.tearDown();
  }

  public void testConcurrentRequestsHitAllServers() throws Exception {
    int concurrentRequests = 5;
    lbSolrServer = new BackupRequestLBHttpSolrClient(httpClient, commExecutor, concurrentRequests, 0, true, "TestRegistry", BackupRequestLBHttpSolrClient.BackupPercentile.NONE);
    List<String> serverList = new ArrayList<String>();
    for (int i = 0; i < concurrentRequests; i++) {
      SolrInstance si = new SolrInstance("solr/collection1" + i, 0);
      si.setUp();
      si.startJetty();
      serverList.add(si.getUrl());
      addDocs(si);
      solr.put("solr/collection1" + i, si);
    }

    SolrQuery solrQuery = new SolrQuery("*:*");
    QueryResponse resp = null;
    QueryRequest request = new QueryRequest(solrQuery);

    long requestCountBeforeRequest = 0;
    for (SolrInstance si : solr.values()) {
      requestCountBeforeRequest += si.jetty.getDebugFilter().getTotalRequests();
    }

    resp = submitRequest(lbSolrServer, serverList, request);
    while(commExecutor.getActiveCount()>0) Thread.sleep(1); //give servers a chance;

    assertEquals(10, resp.getResults().getNumFound());

    long requestCountAfterRequest = 0;
    for (SolrInstance si : solr.values()) {
      requestCountAfterRequest += si.jetty.getDebugFilter().getTotalRequests();
    }

    assertEquals(requestCountAfterRequest - requestCountBeforeRequest,
        concurrentRequests);
  }

  public void testTimeoutExceededTurnsServerZombie() throws Exception {

    LBHttpSolrClient lbSolrClient = new BackupRequestLBHttpSolrClient(
        httpClient, commExecutor, 3, 250, true, "TestRegistry", BackupRequestLBHttpSolrClient.BackupPercentile.NONE);
    List<String> serverList = new ArrayList<String>();

    SolrInstance slow = new SolrInstance("solr/slow", 0);
    slow.setUp();
    slow.startJetty(750);
    serverList.add(slow.getUrl());
    addDocs(slow);
    solr.put("solr/slow", slow);

    SolrInstance si = new SolrInstance("solr/normal", 0);
    si.setUp();
    si.startJetty();
    serverList.add(si.getUrl());
    addDocs(si);
    solr.put("solr/normal", si);

    HttpClientUtil.setSoTimeout(httpClient, 500);

    SolrQuery solrQuery = new SolrQuery("*:*");
    QueryResponse resp = null;
    QueryRequest request = new QueryRequest(solrQuery);

    resp = submitRequest(lbSolrClient, serverList, request);

    assertEquals(10, resp.getResults().getNumFound());
    String name = resp.getResults().get(0).getFieldValue("name").toString();
    assertEquals("solr/normal", name);

    while(commExecutor.getActiveCount()>0) Thread.sleep(1); // allow timeout to be exceeded.

    //slowFilter.setSleepTime(0);
    SlowJettySolrRunner.setDelay(0);

    long requestCountBeforeRequest = slow.jetty.getDebugFilter()
        .getTotalRequests();

    resp = submitRequest(lbSolrClient, serverList, request);
    assertEquals(10, resp.getResults().getNumFound());
    name = resp.getResults().get(0).getFieldValue("name").toString();

    assertEquals(requestCountBeforeRequest, slow.jetty.getDebugFilter()
        .getTotalRequests());

    assertEquals("solr/normal", name);

  }

  public void testTimeoutExceeded() throws Exception {

    LBHttpSolrClient lbSolrClient = new BackupRequestLBHttpSolrClient(
        httpClient, commExecutor, 2, 0, false, "TestRegistry", BackupRequestLBHttpSolrClient.BackupPercentile.NONE);
    List<String> serverList = new ArrayList<String>();

    for (int i = 0; i < 2; i++) {
      SolrInstance si = new SolrInstance("solr/collection1" + i, 0);
      si.setUp();
      si.startJetty(501);
      serverList.add(si.getUrl());
      addDocs(si);
      solr.put("solr/collection1" + i, si);
    }
    lbSolrClient.setAliveCheckInterval(750);

    SolrQuery solrQuery = new SolrQuery("*:*");
    QueryResponse resp = null;
    QueryRequest request = new QueryRequest(solrQuery);

    try {
      HttpClientUtil.setSoTimeout(httpClient, 250);
      resp = submitRequest(lbSolrClient, serverList, request);
    } catch (SolrServerException ex) {
      assertNotNull(ex);
    }
    long requestCountBeforeRequest = solr.get("solr/collection10").jetty.getDebugFilter().getTotalRequests()
                                    + solr.get("solr/collection11").jetty.getDebugFilter().getTotalRequests();
    try {
      HttpClientUtil.setSoTimeout(httpClient, 250);
      resp = submitRequest(lbSolrClient, serverList, request);
    } catch (SolrServerException ex) {
      assertNotNull(ex);
    }
    long requestCountAfterRequest = solr.get("solr/collection10").jetty.getDebugFilter().getTotalRequests()
        + solr.get("solr/collection11").jetty.getDebugFilter() .getTotalRequests();

    assertEquals(requestCountBeforeRequest,requestCountAfterRequest);

    HttpClientUtil.setSoTimeout(httpClient, 0);
    //slowFilter.setSleepTime(0);
    SlowJettySolrRunner.setDelay(0);

    Thread.sleep(1000);

    while(commExecutor.getActiveCount()>0) Thread.sleep(1);

    resp = submitRequest(lbSolrClient, serverList, request);
    assertEquals(10, resp.getResults().getNumFound());

  }

  public void testBackupRequest() throws Exception {

    LBHttpSolrClient lbSolrClient = new BackupRequestLBHttpSolrClient(
        httpClient, commExecutor, 2, 200, true, "TestRegistry", BackupRequestLBHttpSolrClient.BackupPercentile.NONE);
    List<String> serverList = new ArrayList<String>();

    SolrInstance slow = new SolrInstance("solr/collection10", 0);
    slow.setUp();
    slow.startJetty(300);
    serverList.add(slow.getUrl());
    addDocs(slow);
    solr.put("solr/collection10", slow);

    SolrInstance fast = new SolrInstance("solr/collection11", 0);
    fast.setUp();
    fast.startJetty();
    serverList.add(fast.getUrl());
    addDocs(fast);
    solr.put("solr/collection11", fast);

    SolrQuery solrQuery = new SolrQuery("*:*");
    QueryResponse resp = null;
    QueryRequest request = new QueryRequest(solrQuery);
    long requestCountBeforeRequest = slow.jetty.getDebugFilter() .getTotalRequests();

    resp = submitRequest(lbSolrClient, serverList, request);
    assertEquals(10, resp.getResults().getNumFound());
    String name = resp.getResults().get(0).getFieldValue("name").toString();
    assertEquals("solr/collection11", name);

    while(commExecutor.getActiveCount()>0) Thread.sleep(1); // wait for slow filter to stop sleeping.

    assertEquals(slow.jetty.getDebugFilter().getTotalRequests()
        - requestCountBeforeRequest, 1);

    //slowFilter.setSleepTime(0);
    SlowJettySolrRunner.setDelay(0);

    resp = submitRequest(lbSolrClient, serverList, request);
    assertEquals(10, resp.getResults().getNumFound());
    name = resp.getResults().get(0).getFieldValue("name").toString();
    assertEquals("solr/collection10", name);

  }

  public void testBackupRequestPercentile() throws Exception {
    String sharedRegistryName = "testBackupRequestPercentile";
    MetricRegistry sharedRegistry = SharedMetricRegistries.getOrCreate(sharedRegistryName);

    LBHttpSolrClient lbSolrClient = new BackupRequestLBHttpSolrClient(
            httpClient, commExecutor, 2, -1, true, sharedRegistryName, BackupRequestLBHttpSolrClient.BackupPercentile.NONE);
    List<String> serverList = new ArrayList<String>();

    SolrInstance slow = new SolrInstance("solr/collection10", 0);
    slow.setUp();
    slow.startJetty(90);
    serverList.add(slow.getUrl());
    addDocs(slow);
    solr.put("solr/collection10", slow);

    SolrInstance fast = new SolrInstance("solr/collection11", 0);
    fast.setUp();
    fast.startJetty();
    serverList.add(fast.getUrl());
    addDocs(fast);
    solr.put("solr/collection11", fast);

    QueryRequest requestP50 = percentileRequest("P50");
    QueryRequest requestP999 = percentileRequest("P999");

    // initialize the percentile tracking by doing percentile-enabled requests
    while (!sharedRegistry.getGauges().keySet().contains(BackupRequestLBHttpSolrClient.cachedGaugeName("testPerformanceClass", BackupRequestLBHttpSolrClient.BackupPercentile.P50))) {
      submitRequest(lbSolrClient, serverList, requestP50);
    }
    while (!sharedRegistry.getGauges().keySet().contains(BackupRequestLBHttpSolrClient.cachedGaugeName("testPerformanceClass", BackupRequestLBHttpSolrClient.BackupPercentile.P999))) {
      submitRequest(lbSolrClient, serverList, requestP999);
    }

    // establish some performance history - uniform distribution between 1 and 100 ms,
    // (although possibly skewed somewhat by the initialization queries above)
    // which puts the "slow" instance at the 90th percentile.
    Timer timer = sharedRegistry.timer(BackupRequestLBHttpSolrClient.prefixedMetric("testPerformanceClass"));
    Random randomGenerator = new Random();
    for(int i = 0; i<500; i++) timer.update(randomGenerator.nextInt(100) + 1, TimeUnit.MILLISECONDS);

    Thread.sleep(15000); // allow the percentile cache to get updated

    long requestCountBeforeRequest;
    QueryResponse resp = null;
    String name = null;

    System.out.println("Timers: " + sharedRegistry.getTimers().keySet().toString());
    System.out.println("Median response time: " + timer.getSnapshot().getMedian() / 1000000);
    System.out.println("Gauges: " + sharedRegistry.getGauges().keySet().toString());

    // a request at the p50 should cause a backup request
    requestCountBeforeRequest = slow.jetty.getDebugFilter().getTotalRequests();
    resp = submitRequest(lbSolrClient, serverList, requestP50);
    assertEquals(10, resp.getResults().getNumFound());
    name = resp.getResults().get(0).getFieldValue("name").toString();
    assertEquals("solr/collection11", name);
    while(commExecutor.getActiveCount()>0) Thread.sleep(1); // wait for slow filter to stop sleeping.
    assertEquals(slow.jetty.getDebugFilter().getTotalRequests()
            - requestCountBeforeRequest, 1);

    // a request at the p999 should NOT cause a backup request
    requestCountBeforeRequest = slow.jetty.getDebugFilter().getTotalRequests();
    resp = submitRequest(lbSolrClient, serverList, requestP999);
    assertEquals(10, resp.getResults().getNumFound());
    name = resp.getResults().get(0).getFieldValue("name").toString();
    assertEquals("solr/collection10", name);
    while(commExecutor.getActiveCount()>0) Thread.sleep(1); // wait for slow filter to stop sleeping.
    assertEquals(slow.jetty.getDebugFilter().getTotalRequests()
            - requestCountBeforeRequest, 1);
  }

  public QueryRequest percentileRequest(String percentile) {
    SolrQuery solrQuery = new SolrQuery("*:*");
    solrQuery.set("backupRequestPercentile",percentile);
    solrQuery.set("performanceClass","testPerformanceClass");
    QueryResponse resp = null;
    QueryRequest request = new QueryRequest(solrQuery);
    return request;
  }



  public void testBackupRequestBothSlow() throws Exception {

    LBHttpSolrClient lbSolrClient = new BackupRequestLBHttpSolrClient(
        httpClient, commExecutor, 2, 250, true, "TestRegistry", BackupRequestLBHttpSolrClient.BackupPercentile.NONE);
    List<String> serverList = new ArrayList<String>();

    SolrInstance slow = new SolrInstance("solr/collection10", 0);
    slow.setUp();
    slow.startJetty(500);
    serverList.add(slow.getUrl());
    addDocs(slow);
    solr.put("solr/collection10", slow);

    SolrInstance slowToo = new SolrInstance("solr/collection11", 0);
    slowToo.setUp();
    slowToo.startJetty(500);
    serverList.add(slowToo.getUrl());
    addDocs(slowToo);
    solr.put("solr/collection11", slowToo);

    SolrQuery solrQuery = new SolrQuery("*:*");
    QueryResponse resp = null;
    QueryRequest request = new QueryRequest(solrQuery);

    long requestCountBeforeRequest =
        slow.jetty.getDebugFilter().getTotalRequests()
        + slowToo.jetty.getDebugFilter().getTotalRequests();
    resp = submitRequest(lbSolrClient, serverList, request);
    while(commExecutor.getActiveCount()>0) Thread.sleep(1);
    assertEquals(10, resp.getResults().getNumFound());
    assertEquals( slow.jetty.getDebugFilter().getTotalRequests()
        + slowToo.jetty.getDebugFilter().getTotalRequests()
        - requestCountBeforeRequest, 2);

  }

  public void testSimple() throws Exception {

    List<String> serverList = new ArrayList<String>();
    for (int i = 0; i < 3; i++) {
      SolrInstance si = new SolrInstance("solr/collection1" + i, 0);
      si.setUp();
      si.startJetty();
      serverList.add(si.getUrl());
      addDocs(si);
      solr.put("solr/collection1" + i, si);
    }

    LBHttpSolrClient lbSolrClient = new BackupRequestLBHttpSolrClient(
        httpClient, commExecutor, 1, 1, true, "TestRegistry", BackupRequestLBHttpSolrClient.BackupPercentile.NONE);
    lbSolrClient.setAliveCheckInterval(500);

    SolrQuery solrQuery = new SolrQuery("*:*");
    QueryResponse resp = null;
    QueryRequest request = new QueryRequest(solrQuery);

    resp = submitRequest(lbSolrClient, serverList, request);
    assertEquals(10, resp.getResults().getNumFound());
    String name = resp.getResults().get(0).getFieldValue("name").toString();
    assertEquals("solr/collection10", name);

    // Kill a server and test again
    solr.get("solr/collection10").jetty.stop();
    solr.get("solr/collection10").jetty = null;

    resp = submitRequest(lbSolrClient, serverList, request);
    assertEquals(10, resp.getResults().getNumFound());
    name = resp.getResults().get(0).getFieldValue("name").toString();
    assertEquals("solr/collection11", name);

    solr.get("solr/collection11").jetty.stop();
    solr.get("solr/collection11").jetty = null;

    resp = submitRequest(lbSolrClient, serverList, request);
    assertEquals(10, resp.getResults().getNumFound());
    name = resp.getResults().get(0).getFieldValue("name").toString();
    assertEquals("solr/collection12", name);

    // Start the killed server once again
    solr.get("solr/collection10").startJetty();
    // Wait for the alive check to complete
    Thread.sleep(1200);

    resp = submitRequest(lbSolrClient, serverList, request);
    assertEquals(10, resp.getResults().getNumFound());
    name = resp.getResults().get(0).getFieldValue("name").toString();
    assertEquals("solr/collection10", name);

  }

  public void testExceptionForIllformedQuery() throws Exception {

    List<String> serverList = new ArrayList<String>();

    SolrInstance si = new SolrInstance("solr/collection", 0);
    si.setUp();
    si.startJetty();
    serverList.add(si.getUrl());
    addDocs(si);
    solr.put("solr/collection", si);

    LBHttpSolrClient lbSolrClient = new BackupRequestLBHttpSolrClient(
        httpClient, commExecutor, 1, 1, true, "TestRegistry", BackupRequestLBHttpSolrClient.BackupPercentile.NONE);

    SolrQuery solrQuery = new SolrQuery("not_a_field::*");
    QueryResponse resp = null;
    QueryRequest request = new QueryRequest(solrQuery);
    SolrServerException exc = null;
    try{
      resp = submitRequest(lbSolrClient, serverList, request);
    }catch(SolrServerException ex){
      exc = ex;
    }
    assertNotNull(exc);

  }

  private QueryResponse submitRequest(LBHttpSolrClient lbSolrServer,
      List<String> serverList, QueryRequest request)
      throws SolrServerException, IOException {
    return new QueryResponse(lbSolrServer.request(
        new LBHttpSolrServer.Req(request, serverList)).getResponse(),
        lbSolrServer);
  }

  private class SolrInstance {
    String name;
    File homeDir;
    File dataDir;
    File confDir;
    int port;
    JettySolrRunner jetty;

    public SolrInstance(String name, int port) {
      File home = LuceneTestCase.createTempDir(getClass().getName() + "-"
              + System.currentTimeMillis()).toFile();

      this.homeDir = new File(home, name);
      this.name = name;
      this.port = port;
      dataDir = new File(homeDir + "/collection1", "data");
      confDir = new File(homeDir + "/collection1", "conf");
    }

    public String getHomeDir() {
      return homeDir.toString();
    }

    public String getUrl() {
      return "http://127.0.0.1:" + port + "/solr/collection1";
    }

    public String getSchemaFile() {
      return "solrj/solr/collection1/conf/schema-replication1.xml";
    }

    public String getConfDir() {
      return confDir.toString();
    }

    public String getDataDir() {
      return dataDir.toString();
    }

    public String getSolrConfigFile() {
      return "solrj/solr/collection1/conf/solrconfig-slave1.xml";
    }

    public String getSolrXmlFile() { return "solrj/solr/solr.xml"; }

    public void setUp() throws Exception {
      homeDir.mkdirs();
      dataDir.mkdirs();
      confDir.mkdirs();

      File f = new File(confDir, "solrconfig.xml");
      FileUtils.copyFile(SolrTestCaseJ4.getFile(getSolrConfigFile()), f);
      f = new File(confDir, "schema.xml");
      FileUtils.copyFile(SolrTestCaseJ4.getFile(getSchemaFile()), f);
      f = new File(homeDir, "solr.xml");
      FileUtils.copyFile(SolrTestCaseJ4.getFile(getSolrXmlFile()), f);
      Files.createFile(homeDir.toPath().resolve("collection1/core.properties"));
    }

    public void tearDown() throws Exception {
      if (jetty != null) jetty.stop();
      IOUtils.rm(homeDir.toPath());
    }

    public void startJetty() throws Exception {
      startJetty(-1);
    }

    public void startJetty(int delay) throws Exception {
      Properties props = new Properties();
      props.setProperty("solrconfig", "bad_solrconfig.xml");
      props.setProperty("solr.data.dir", getDataDir());

      JettyConfig.Builder jettyConfigBuilder = JettyConfig.builder().setContext("/solr").setPort(port);
      jetty = new SlowJettySolrRunner(getHomeDir(), props, jettyConfigBuilder, delay);

      jetty.start();
      int newPort = jetty.getLocalPort();
      if (port != 0 && newPort != port) {
        fail("TESTING FAILURE: could not grab requested port.");
      }
      this.port = newPort;
    }
  }

}
