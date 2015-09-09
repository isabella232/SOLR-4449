package org.apache.solr.client.solrj;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

public class SlowFilter implements Filter {
  int initialDelay = 0;

  public SlowFilter() {
    initialDelay = SlowJettySolrRunner.getDelay();
  }

  public void destroy() {}

  public void doFilter(ServletRequest servletRequest,
                       ServletResponse servletResponse, FilterChain filterChain)
          throws IOException, ServletException {
    try {
      HttpServletRequest req = (HttpServletRequest) servletRequest;
      if (req.getMethod().equals("GET") && initialDelay > 0) {
        Thread.sleep(SlowJettySolrRunner.getDelay());
      }
    } catch (InterruptedException e) {
      throw new RuntimeException();
    }
    filterChain.doFilter(servletRequest, servletResponse);
  }

  public void init(FilterConfig arg0) throws ServletException {}
}

