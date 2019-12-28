######################################################
#
#
#  This script is used for UDP event data.
#
#
#
#
#
#   Ji Guo
#   Learning Analytics Specialist
#   Research & Analytics
#   Office of Teaching, Learning, & Technology
#   University of Iowa
#
#
#
######################################################



import pandas as pd
from datetime import datetime
from dateutil import tz
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly.express as px
import plotly.tools as plotly_tools
import plotly.graph_objs as go
from plotly.offline import plot
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import gaussian_kde
from IPython.display import HTML
import pandas_gbq


def engage(course_sis_id, project_id = 'udp-uiowa-prod'):
    query = """SELECT id, action, event_time, type, ed_app,
                  	 CAST(JSON_EXTRACT_SCALAR(event,'$.action') AS STRING) AS event_type, 
                  	 CAST(JSON_EXTRACT_SCALAR(event,'$.federatedSession.messageParameters.custom_canvas_course_id') AS STRING) AS course_id,   
                  	 CAST(JSON_EXTRACT_SCALAR(event,'$.federatedSession.messageParameters.custom_canvas_user_id') AS STRING) AS canvas_user_id,
                  	 CAST(JSON_EXTRACT_SCALAR(event,'$.federatedSession.messageParameters.lis_course_offering_sourcedid') AS STRING) AS sis_source_id,  
                     CAST(JSON_EXTRACT_SCALAR(event,'$.federatedSession.messageParameters.lis_person_sourcedid') AS STRING) AS university_id,
                     CAST(JSON_EXTRACT_SCALAR(event,'$.group.academicSession') AS STRING) AS semester,  
                     CAST(JSON_EXTRACT_SCALAR(event,'$.object.isPartOf.name') AS STRING) AS title,     
                     CAST(JSON_EXTRACT_SCALAR(event,'$.object.isPartOf.type') AS STRING) AS document_type, 
                     CAST(JSON_EXTRACT_SCALAR(event,'$.object.name') AS STRING) AS page
               FROM event_store.events 
               WHERE ed_app = 'https://engage.unizin.org'
               AND JSON_EXTRACT_SCALAR(event,'$.federatedSession.messageParameters.lis_course_offering_sourcedid') = '%s'"""%(course_sis_id)
    data = pandas_gbq.read_gbq(query, project_id=project_id)
    return data
    







def main():
    engage()

if __name__ == '__main__':
    main()