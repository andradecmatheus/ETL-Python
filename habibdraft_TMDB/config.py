#config.py
api_key = "47bedb51650fedbf12b6a1ac4ab1ae6b"

# to make sure you don’t put it directly into your source code. 
# One quick way to do this is to create a file called config.py in the same directory you will be creating your ETL script in.


#### If you’re publishing your code anywhere, ####
# 
# you should put your config.py into a .gitignore or similar file to make sure it doesn’t get pushed to any remote repositories. Y
# ou can also store the API key as an environment variable, or use another method to hide it. 
# The goal is to protect it from being exposed in the ETL script.