package org.apache.solr.client.solrj;


import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.eclipse.jetty.servlet.ServletHolder;

import java.util.Properties;
import java.util.SortedMap;

public class SlowJettySolrRunner extends JettySolrRunner {
  /**
   * WARNING: Delay is SHARED across all instances!
   * (except those created with delay -1, who have no delay, and whose delay can never be changed)
   */
  volatile static int delay = 0;
  public static void setDelay(int newDelay) {
    delay = newDelay;
  }
  public static int getDelay() {
    return delay;
  }

  public SlowJettySolrRunner(String solrHome, Properties nodeProperties, JettyConfig.Builder config, int initDelay) {
    super(solrHome, nodeProperties, finalizeBuilder(config, initDelay));
  }

  private static JettyConfig finalizeBuilder(JettyConfig.Builder config, int initDelay) {
    if (initDelay >= 0) {
      setDelay(initDelay);
      config.withFilter(SlowFilter.class, "*");
    }
    return config.build();
  }


}
