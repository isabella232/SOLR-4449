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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
                                       ThreadPoolExecutor threadPoolExecuter, int defaultMaximumConcurrentRequests,
                                       int backUpRequestPause, boolean tryDeadServers) throws MalformedURLException {
    super(httpClient);
    this.threadPoolExecuter = threadPoolExecuter;
    this.defaultMaximumConcurrentRequests = defaultMaximumConcurrentRequests;
    this.defaultBackUpRequestDelay = backUpRequestPause;
    this.tryDeadServers = tryDeadServers;
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
    int backupDelay = reqParams == null
            ? defaultBackUpRequestDelay
            : reqParams.getInt(HttpBackupRequestShardHandlerFactory.BACKUP_REQUEST_DELAY, defaultBackUpRequestDelay);

    // If we can't do anything useful, fall back to the stock solr code
    if (maximumConcurrentRequests < 0 || backupDelay < 0) {
      return super.request(req);
    }

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
      Callable<RequestTaskState> task = createRequestTask(client, req, isUpdate, false, null);
      executer.submit(task);
      inFlight++;
      returnedRsp = getResponseIfReady(executer, patience(inFlight, maximumConcurrentRequests, backupDelay));
      if (returnedRsp == null) {
        // null response signifies that the response took too long.
          log.info("Server :{} did not respond before the backupRequestDelay time of {} elapsed", client.baseUrl, backupDelay);
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
          Callable<RequestTaskState> task = createRequestTask(wrapper.client, req, isUpdate, true, wrapper.getKey());
          executer.submit(task);
          inFlight++;
          returnedRsp = getResponseIfReady(executer, patience(inFlight, maximumConcurrentRequests, backupDelay));
          if (returnedRsp == null) {
            log.info("Server :{} did not respond before the backupRequestDelay time of {} elapsed", wrapper.getKey(), backupDelay);
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
                                                       final boolean isZombie, final String zombieKey) {

    Callable<RequestTaskState> task = new Callable<RequestTaskState>() {
      public RequestTaskState call() throws Exception {
        Rsp rsp = new Rsp();
        rsp.server = client.getBaseURL();
        RequestTaskState taskState = new RequestTaskState();
        taskState.response = rsp;

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
