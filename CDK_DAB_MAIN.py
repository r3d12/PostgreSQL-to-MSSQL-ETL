import DELTA_MERGE
import BULK_DAB
import sys
import ArchiveAndEmailLogs
import logging
from configparser import ConfigParser

file = 'config.ini'
config = ConfigParser()
config.read(file)



interval = config['interval']['runat']

#bulk update smaller tables
for table in list(config['bulk_tables']):
    try:
        BULK_DAB.BULK(config['bulk_tables'][table])
    except:#if fail move onto next table
        pass

#update delta tables from x hours and merge to main tables
for table in list(config['delta_tables']):
    try:
        DELTA_MERGE.DeltaMerge(config['delta_tables'][table], interval)
    except:#if fail move onto next table
        pass



#archive log and send emails
emails =[]
for email in list(config['emails']):
    emails.append(config['emails'][email])

logging.shutdown()    
ArchiveAndEmailLogs.logHandler(emails)



