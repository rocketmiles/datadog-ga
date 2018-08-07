"""
Google Analytics check
Collects metrics from the Analytics API.
Jonathan Makuc - Bithaus Chile (Datadog Partner) - jmakuc@bithaus.cl

2016-04-13
- Support for pageViews metric
- Metric value is read from "1 minute ago" instead of "during the last minute"
  in order to obtain a consistent value to report to Datadog. Using "during the last 
  minute" result in reading zeros while waiting for visitors to view pages in that time 
  frame.
- Dimensions and tags can be controlled on yaml file


"""

from checks import AgentCheck, CheckException

#from apiclient.discovery import build
from googleapiclient.discovery import build

import google.auth
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession

import httplib2
import time
from pprint import pprint

MINUTES_AGO_METRIC = "rt:minutesAgo"

class GoogleAnalyticsCheck(AgentCheck):
  """ Collects as many metrics as instances defined in ga.yaml
  """

  scopes = ['https://www.googleapis.com/auth/analytics.readonly']
  service = 0
  api_name = 'analytics'
  api_version = 'v3'

  def __init__(self, *args, **kwargs):
    AgentCheck.__init__(self, *args, **kwargs)
    self.log.info('service_account_email: %s' % self.init_config.get('service_account_email'))
    self.log.info('key_file_location: %s' % self.init_config.get('key_file_location'))
    self.credentials = credentials = service_account.Credentials.from_service_account_file(self.init_config.get('key_file_location'))
    self.service = build(self.api_name, self.api_version, credentials=self.credentials)    

  
  def check(self, instance):
    self.log.info('profile: %s, tags: %s, pageview_dimensions: %s' % (instance.get('profile'), instance.get('tags'), instance.get('pageview_dimensions')))        
    
    profile = instance.get('profile')
    instanceTags = instance.get('tags')
    instanceTags.append("profile:" + profile)
    metricType = instance.get('metricType') or 'gauge'
    metricName = instance.get('metricName')
    gaMetricName = instance.get('gaMetricName')
    gagaMetricName = instance.get('gagaMetricName')
    dimensions = instance.get('dimensions')
    self.log.info("Getting metric {0} with dimensions {1}".format(gaMetricName, dimensions))
    result = self.get_results(profile, gaMetricName, dimensions)
    headers = result.get('columnHeaders')
    rows = result.get('rows')

    print result

    if len(rows) < 1:
      print "No rows returned, no metrics sent"
      return
      
    metricsSent = 0
    minutesAgoMetric = MINUTES_AGO_METRIC in dimensions
    minutesAgoIndex = -1
    if (minutesAgoMetric):
      minutesAgoColumn = filter(lambda header: header["name"] == MINUTES_AGO_METRIC, headers)[0]
      minutesAgoIndex = headers.index(minutesAgoColumn)
      ts = time.time() - 60
      print "minutesAgoIndex found to be {0}".format(minutesAgoIndex)
    else:
      ts = time.time()

    for row in rows:
      # In order to have a consistent metric, we look for the value 1 minute ago
      # and not during the last minute.
      if minutesAgoMetric and int(row[minutesAgoIndex]) != 1:
        self.log.info("Skipping row because minutesAgo dimension is {0} (only checking 1)".format(row[minutesAgoIndex]))
        continue

      tags = []
      tags.extend(instanceTags)
      for i in xrange(len(headers)-1):
        if i != minutesAgoIndex:
          # we remove the "rt" from the dimension name
          tags.append(headers[i].get('name')[3:] + ":" + row[i])
          
      if (metricType == "gauge"):
        self.gauge(metricName, int(row[-1]), tags, None, None)
      elif (metricType == "count"):
        self.count(metricName, int(row[-1]), tags, None, None)
      else:
        print "UNKNOWN METRIC TYPE"
      
      metricsSent = metricsSent + 1
        
    self.log.info("{0} sent: {1}".format(gaMetricName, metricsSent));        

    
  def get_results(self, profile_id, the_metric, dims):
    if len(dims) > 0:
      return self.service.data().realtime().get(
        ids=profile_id,
        metrics=the_metric,
        dimensions=','.join(dims)).execute() 
    else:
      return self.service.data().realtime().get(
        ids=profile_id,
        metrics=the_metric).execute() 
      