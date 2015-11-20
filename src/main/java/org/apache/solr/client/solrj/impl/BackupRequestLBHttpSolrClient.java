package org.apache.solr.client.solrj.impl;

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

import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.IsUpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.component.HttpBackupRequestShardHandlerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class BackupRequestLBHttpSolrClient extends LBHttpSolrClient {
  private static Logger log = LoggerFactory.getLogger(BackupRequestLBHttpSolrClient.class);
  private final int defaultMaximumConcurrentRequests;
  private final int defaultBackUpRequestDelay;
  private final ThreadPoolExecutor threadPoolExecuter;
  private final boolean tryDeadServers;
  private final MetricRegistry registry;

  public enum BackupPercentile {
    NONE, P50, P75, P90, P95, P98, P99, P999
  }
  private final BackupPercentile defaultBackupPercentile;

  public static BackupPercentile getPercentile(String percentile) {
    return BackupPercentile.valueOf(percentile.toUpperCase().trim());
  }

  private enum TaskState {
    ResponseReceived, ServerException, RequestException
  }
  private class RequestTaskState {
    private Exception exception;
    private TaskState stateDescription;
    private Rsp response;

    void setException(Exception exception, TaskState stateDescription) {
      this.exception = exception;
      this.stateDescription = stateDescription;
    }
  }

  public BackupRequestLBHttpSolrClient(HttpClient httpClient,
                                       ThreadPoolExecutor threadPoolExecuter,
                                       int defaultMaximumConcurrentRequests,
                                       int backUpRequestPause,
                                       boolean tryDeadServers,
                                       String registryName,
                                       BackupPercentile defaultBackupPercentile) throws MalformedURLException {
    super(httpClient);
    this.threadPoolExecuter = threadPoolExecuter;
    this.defaultMaximumConcurrentRequests = defaultMaximumConcurrentRequests;
    this.defaultBackUpRequestDelay = backUpRequestPause;
    this.tryDeadServers = tryDeadServers;
    this.registry = SharedMetricRegistries.getOrCreate(registryName);
    this.defaultBackupPercentile = defaultBackupPercentile;
  }

  /**
   * Tries to query a live server from the list provided in Req. Servers in the
   * dead pool are skipped. If a request fails due to an IOException, the server
   * is moved to the dead pool for a certain period of time, or until a test
   * request on that server succeeds.
   *
   * If a request takes longer than defaultBackUpRequestDelay the request will be sent
   * to the next server in the list, this will continue until there is a
   * response, the server list is exhausted or the number of requests in flight
   * equals defaultMaximumConcurrentRequests.
   *
   * Servers are queried in the exact order given (except servers currently in
   * the dead pool are skipped). If no live servers from the provided list
   * remain to be tried, a number of previously skipped dead servers will be
   * tried. Req.getNumDeadServersToTry() controls how many dead servers will be
   * tried.
   *
   * If no live servers are found a SolrServerException is thrown.
   *
   * @param req
   *          contains both the request as well as the list of servers to query
   *
   * @return the result of the request
   */
  @Override
  public Rsp request(Req req) throws SolrServerException, IOException {
    SolrParams reqParams = req.getRequest().getParams();

    int maximumConcurrentRequests = reqParams == null
            ? defaultMaximumConcurrentRequests
            : reqParams.getInt(HttpBackupRequestShardHandlerFactory.MAX_CONCURRENT_REQUESTS, defaultMaximumConcurrentRequests);

    // If we can't do anything useful, fall back to the stock solr code
    if (maximumConcurrentRequests < 0) {
      return super.request(req);
    }

    // if there's an explicit backupDelay in the request, use that
    int backupDelay = reqParams == null
            ? -1
            : reqParams.getInt(HttpBackupRequestShardHandlerFactory.BACKUP_REQUEST_DELAY, -1);

    BackupPercentile backupPercentile = defaultBackupPercentile;
    String backupPercentileParam = reqParams == null
            ? null
            : reqParams.get(HttpBackupRequestShardHandlerFactory.BACKUP_PERCENTILE);
    if (backupPercentileParam != null) {
      backupPercentile = getPercentile(backupPercentileParam);
    }

    String performanceClass = reqParams == null
            ? req.getRequest().getPath()  // getPath is typically the request handler name
            : reqParams.get(HttpBackupRequestShardHandlerFactory.PERFORMANCE_CLASS, reqParams.get(CommonParams.QT, req.getRequest().getPath()));  // TODO: Is QT getting filtered out of the distrib requests?

    if (backupDelay < 0 && backupPercentile != BackupPercentile.NONE) {
      // no explicit backup delay, consider a backup percentile for the delay.
      double rate = getCachedRate(performanceClass);
      if (rate > 0.1) {   // 1 request per 10 seconds minimum.
        backupDelay = getCachedPercentile(performanceClass, backupPercentile);
        log.debug("Using delay of {}ms for percentile {} for performanceClass {}", backupDelay, backupPercentile.name(), performanceClass);
      }
      else {
        log.info("Insufficient query rate ({} per sec) to rely on latency percentiles for performanceClass {}", rate, performanceClass);
      }
    }
    else {
      // not using a percentile to track backupDelay
      performanceClass = null;
    }

    if (backupDelay < 0) {
      backupDelay = defaultBackUpRequestDelay;
    }

    // If we are using a backupPercentile, we need to proceed regardless of backupDelay so we can record and build the percentile info.
    // If not, and we still don't have a backupDelay, fall back to stock solr code.
    if (backupPercentile == BackupPercentile.NONE && backupDelay < 0) {
      return super.request(req);
    }

    // Reaching this point with a backupDelay < 0 means backup requests are effectively disabled, but we're executing
    // this codepath anyway. Presumably in order to build latency percentile data for future requests.

    ArrayBlockingQueue<Future<RequestTaskState>> queue = new ArrayBlockingQueue<Future<RequestTaskState>>(maximumConcurrentRequests +1);
    ExecutorCompletionService<RequestTaskState> executer =
            new ExecutorCompletionService<RequestTaskState>(threadPoolExecuter, queue);

    final int numDeadServersToTry = req.getNumDeadServersToTry();
    final boolean isUpdate = req.getRequest() instanceof IsUpdateRequest;
    List<ServerWrapper> skipped = null;
    int inFlight = 0;
    RequestTaskState returnedRsp = null;
    Exception ex = null;

    long timeAllowedNano = getTimeAllowedInNanos(req.getRequest());
    long timeOutTime = System.nanoTime() + timeAllowedNano;

    for (String serverStr : req.getServers()) {
      if(isTimeExceeded(timeAllowedNano, timeOutTime)) {
        break;
      }

      serverStr = normalize(serverStr);
      // if the server is currently a zombie, just skip to the next one
      ServerWrapper wrapper = zombieServers.get(serverStr);
      if (wrapper != null) {
        if (tryDeadServers && numDeadServersToTry > 0) {
          if (skipped == null) {
            skipped = new ArrayList<>(numDeadServersToTry);
            skipped.add(wrapper);
          }
          else if (skipped.size() < numDeadServersToTry) {
            skipped.add(wrapper);
          }
        }

        continue;
      }
      HttpSolrClient client = makeSolrClient(serverStr);
      Callable<RequestTaskState> task = createRequestTask(client, req, isUpdate, false, null, performanceClass);
      executer.submit(task);
      inFlight++;

      returnedRsp = getResponseIfReady(executer, patience(inFlight, maximumConcurrentRequests, backupDelay));
      if (returnedRsp == null) {
        // null response signifies that the response took too long.
          log.debug("Server :{} did not respond before the backupRequestDelay time of {} elapsed", client.baseUrl, backupDelay);
        continue;
      }
      inFlight--;
      if (returnedRsp.stateDescription == TaskState.ResponseReceived) {
        return returnedRsp.response;
      } else if (returnedRsp.stateDescription == TaskState.ServerException) {
        ex = returnedRsp.exception;
      } else if (returnedRsp.stateDescription == TaskState.RequestException) {
        throw new SolrServerException(returnedRsp.exception);
      }
    }

    // no response so try the zombie servers
    if(tryDeadServers && skipped != null){
      if (returnedRsp == null || returnedRsp.stateDescription == TaskState.ServerException) {
        // try the servers we previously skipped
        for (ServerWrapper wrapper : skipped) {
          if(isTimeExceeded(timeAllowedNano, timeOutTime)) {
            break;
          }
          Callable<RequestTaskState> task = createRequestTask(wrapper.client, req, isUpdate, true, wrapper.getKey(), performanceClass);
          executer.submit(task);
          inFlight++;
          returnedRsp = getResponseIfReady(executer, patience(inFlight, maximumConcurrentRequests, backupDelay));
          if (returnedRsp == null) {
            log.debug("Server :{} did not respond before the backupRequestDelay time of {} elapsed", wrapper.getKey(), backupDelay);
            continue;
          }
          inFlight--;
          if (returnedRsp.stateDescription == TaskState.ResponseReceived) {
            return returnedRsp.response;
          } else if (returnedRsp.stateDescription == TaskState.ServerException) {
            ex = returnedRsp.exception;
          } else if (returnedRsp.stateDescription == TaskState.RequestException) {
            throw new SolrServerException(returnedRsp.exception);
          }
        }
      }
    }

    // All current attempts could be slower than backUpRequestPause or returned
    // response could be from struggling server
    // so we need to wait until we get a good response or tasks all are
    // exhausted.
    if (returnedRsp == null || returnedRsp.stateDescription == TaskState.ServerException) {
      while (inFlight > 0) {
        returnedRsp = getResponseIfReady(executer, -1);
        inFlight--;
        if (returnedRsp.stateDescription == TaskState.ResponseReceived) {
          return returnedRsp.response;
        } else if (returnedRsp.stateDescription == TaskState.ServerException) {
          ex = returnedRsp.exception;
        } else if (returnedRsp.stateDescription == TaskState.RequestException) {
          throw new SolrServerException(returnedRsp.exception);
        }
      }
    }

    if (ex == null) {
      throw new SolrServerException(
          "No live SolrServers available to handle this request");
    } else {
      throw new SolrServerException(
          "No live SolrServers available to handle this request:"
              + zombieServers.keySet(), ex);
    }
  }

  private int patience(int inFlight, int maxConcurrentRequests, int waitTime) {
    if (inFlight >= maxConcurrentRequests) {
      return -1;  // wait forever
    }
    return waitTime;
  }

  @Override
  protected Exception addZombie(HttpSolrClient server, Exception e) {
    CharArrayWriter cw = new CharArrayWriter();
    PrintWriter pw = new PrintWriter(cw);
    e.printStackTrace(pw);
    pw.flush();
    String stack = cw.toString();
    log.info("Server :{} did not respond correctly or timed out, the server is zombied. {}", server.getBaseURL(), e.toString() + stack);
    return super.addZombie(server, e);
  }

  private Callable<RequestTaskState> createRequestTask(final HttpSolrClient client, final Req req, final boolean isUpdate,
                                                       final boolean isZombie, final String zombieKey, final String performanceClass) {

    Callable<RequestTaskState> task = new Callable<RequestTaskState>() {
      public RequestTaskState call() throws Exception {
        Rsp rsp = new Rsp();
        rsp.server = client.getBaseURL();
        RequestTaskState taskState = new RequestTaskState();
        taskState.response = rsp;

        Timer.Context timerContext = null;
        if (performanceClass != null)
          timerContext = getTimer(performanceClass).time();

        try {
          MDC.put("LBHttpSolrClient.url", client.getBaseURL());
          Exception ex = doRequest(client, req, rsp, isUpdate, isZombie, zombieKey);
          if (ex != null) {
            taskState.setException(ex, TaskState.ServerException);
          } else {
            taskState.stateDescription = TaskState.ResponseReceived;
          }
        } catch (Exception e) {
          taskState.setException(e, TaskState.RequestException);
        } finally {
          MDC.remove("LBHttpSolrClient.url");
          if (timerContext != null)
            timerContext.stop();
        }

        return taskState;
      }
    };
    return task;
  }

  private RequestTaskState getResponseIfReady(ExecutorCompletionService<RequestTaskState> executer,
      int backupDelay) throws SolrException {

    Future<RequestTaskState> taskInProgress = null;
    try {
      if (backupDelay < 0) {
        taskInProgress = executer.take();
      } else {
        taskInProgress = executer.poll(backupDelay, TimeUnit.MILLISECONDS);
      }
      // could be null if poll time exceeded in which case return null.
      if ( taskInProgress != null && !taskInProgress.isCancelled()) {
        RequestTaskState resp = taskInProgress.get();

        return resp;
      }
    } catch (InterruptedException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } catch (ExecutionException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
    return null;

  }

  private static final String metricsPrefix = "BackupRequestSolrClient.";
  public static String prefixedMetric(String performanceClass) {
    if (performanceClass.startsWith(metricsPrefix))
      return performanceClass;
    else
      return metricsPrefix + performanceClass;
  }

  public static String cachedGaugeName(String metricName, BackupPercentile percentile) {
    return prefixedMetric(metricName + "-cachedpercentile-" + percentile.name());
  }
  public static String cachedRateName(String metricName) {
    return prefixedMetric(metricName + "-cachedrate");
  }

  public Timer getTimer(final String performanceClass) {
    return registry.timer(prefixedMetric(performanceClass));
  }

  /**
   *
   * @param performanceClass
   * @return The 1-minute rate, or -1 if we don't have a viable rate yet
   */
  public double getCachedRate(final String performanceClass) {
    final String cachedRateName = cachedRateName(performanceClass);
    // no getOrCreate here, so watch for race conditions
    Gauge gauge = registry.getGauges().get(cachedRateName);
    if (gauge == null) {
      try {
        registry.register(cachedRateName, new CachedGauge<Double>(15, TimeUnit.SECONDS) {
          Timer t = getTimer(performanceClass);
          @Override
          protected Double loadValue() {
            return t.getOneMinuteRate();
          }
        });
      }
      catch (IllegalArgumentException e) {
        // Got created already by another thread maybe?
        gauge = registry.getGauges().get(cachedRateName);
      }
    }

    if (gauge == null) {
      // It's been created, but hasn't registered yet.
      // Rather than wait around, just try again next time.
      return -1;
    }

    Double rate = (Double)gauge.getValue();
    if (rate == null) {
      // the gauge exists, but hasn't had a chance to tick yet.
      return -1;
    }

    return rate;
  }

  /**
   * @param performanceClass
   * @param percentile
   * @return -1 if we couldn't find a usable percentile, or the number of millis to wait to achieve this percentile
   */
  public int getCachedPercentile(final String performanceClass, final BackupPercentile percentile) {
    if (percentile == BackupPercentile.NONE) {
      return -1;
    }
    final String metricName = prefixedMetric(performanceClass);
    final String cachedGaugeName = cachedGaugeName(metricName, percentile);
    // no getOrCreate here, so watch for race conditions
    Gauge gauge = registry.getGauges().get(cachedGaugeName);
    if (gauge == null) {
      try {
        registry.register(cachedGaugeName,
                new CachedGauge<Integer>(15, TimeUnit.SECONDS) {
                  Timer t = getTimer(metricName);
                  @Override
                  protected Integer loadValue() {
                    double measurementNanos = -1.0;
                    Snapshot snapshot = t.getSnapshot();
                    switch (percentile) {
                      case P50: measurementNanos = snapshot.getMedian(); break;
                      case P75: measurementNanos = snapshot.get75thPercentile(); break;
                      case P90: measurementNanos = snapshot.getValue(0.90); break;
                      case P95: measurementNanos = snapshot.get95thPercentile(); break;
                      case P98: measurementNanos = snapshot.get98thPercentile(); break;
                      case P99: measurementNanos = snapshot.get99thPercentile(); break;
                      case P999: measurementNanos = snapshot.get999thPercentile(); break;
                    }
                    int measurementMs = (int)(measurementNanos / 1000000);
                    if (measurementMs < 1)
                      return -1;
                    else
                      return measurementMs;
                  }
                });
      }
      catch (IllegalArgumentException e) {
        // Got created already by another thread maybe?
        gauge = registry.getGauges().get(cachedGaugeName);
      }
    }
    if (gauge == null) {
      // It's been created, but hasn't registered yet.
      // Rather than wait around, just try again next time.
      return -1;
    }

    Integer msPercentile = (Integer)gauge.getValue();
    if (msPercentile == null) {
      // the gauge exists, but hasn't had a chance to tick yet.
      return -1;
    }

    return msPercentile;
  }


  /**
   * The following was copied from base class (5.3) to work around private access modifiers
   */
  protected long getTimeAllowedInNanos(final SolrRequest req) {
    SolrParams reqParams = req.getParams();
    return reqParams == null ? -1 :
            TimeUnit.NANOSECONDS.convert(reqParams.getInt(CommonParams.TIME_ALLOWED, -1), TimeUnit.MILLISECONDS);
  }
  protected boolean isTimeExceeded(long timeAllowedNano, long timeOutTime) {
    return timeAllowedNano > 0 && System.nanoTime() > timeOutTime;
  }
  protected static Set<Integer> RETRY_CODES = new HashSet<>(4);

  static {
    RETRY_CODES.add(404);
    RETRY_CODES.add(403);
    RETRY_CODES.add(503);
    RETRY_CODES.add(500);
  }



}
