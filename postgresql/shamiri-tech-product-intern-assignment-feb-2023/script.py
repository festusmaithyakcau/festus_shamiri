#  code here  : please structure your code in a way that it is easy to read and understand

# Please read the following instructions carefully and follow them. They are important for the evaluation of your code.
# You are required to write a python script that will read the MOCK_DATA csv file and insert the data into a Postgres database.
# The script should be able to run on any machine and should not require any manual intervention to run.

### Steps:
# 1. Create a Postgres database
# 2. Create a table in the database using the csv file as a reference for the table structure
# 3. Clean the data and insert the data from the csv file into the postgres table 
# 4. Update .env file with the connection details for the database (Note: You can use a local connection or a remote connection * if you are using a remote connection, please provide the connection details in the readme file)
# 5. Update requirements.txt file with the dependencies for the project
# 6. How would you improve the script to make it more efficient? (Note: You are not required to implement the improvements but list them in the readme file)
# 7. How you you ensure the script is running whenever the csv file is updated? (Note: You are not required to implement the improvements but list them in the readme file)
# 8. Upload the final project to github and provide the link to the repository

# Path: script.py

import psycopg2

#connection details for my database

hostname = 'localhost'
database = 'postgres'
username = 'postgres'
pwd = '###########'
port_id = 5432
mycursor = None
myconnection = None

#Connecting to the database
try:
    myconnection = psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id
    )
    # opening the cursor
    mycursor = myconnection.cursor()


    #creating the table
    create_table = '''CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY,
    first_name VARCHAR(20) NOT NULL,
    last_name VARCHAR(20),
    email VARCHAR(30),
    gender VARCHAR(10),
    ip_address VARCHAR(12),
    isAdmin BOOLEAN
    )
    '''
    mycursor.execute(create_table)

    #inserting the data
    #
    #importing csv file
    with open('MOCK_DATA.csv', 'r') as f:
        next(f) #skipping the header row
        mycursor.copy_from(f, 'users', sep=',')
    myconnection.commit()

except Exception as error:
    print(error)
finally:
    if mycursor is not None:
        mycursor.close()
    if myconnection is not None:
        myconnection.close()









