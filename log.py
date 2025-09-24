from datetime import date
from datetime import datetime



logfile ="logfile.txt"

#Log function to write to a text file the processes
def log(message):
 timestamp_format = '%Y-%h-%d-%H:%M:%S' #Year-Monthname-Day-Hour-Minute-Second
 now = datetime.now()
 timestamp = now.strftime(timestamp_format)
 with open("logfile.txt","a") as f:
     f.write(timestamp + ',' + message + '\n')
