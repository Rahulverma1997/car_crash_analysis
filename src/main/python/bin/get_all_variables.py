import os

### Set Environment Variables
os.environ['envn'] = 'TEST'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'
os.environ['user'] = 'sparkuser1'
os.environ['password'] = 'user123'

### Get Environment Variables
envn = os.environ['envn']
header = os.environ['header']
inferSchema = os.environ['inferSchema']
user = os.environ['user']
password = os.environ['password']

### Set Other Variables
appName = "CAR CRASH REPORT"
current_path = os.getcwd()

staging_data = current_path + '/../staging'
output_data = current_path + '/../output'



