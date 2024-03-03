import csv
import mysql.connector
from mysql.connector import errorcode
import os
import sys


cnx = mysql.connector.connect(user='root',
                              password='password',
                              # database='zejian',          
                              # host='127.0.0.1',
                              # error raised in my computer without the line below
                              auth_plugin='mysql_native_password'   
                              )

cursor = cnx.cursor()


# Create a database if does not exsits
def create_database(cursor, DB_NAME):
    try:
        cursor.execute(f"CREATE DATABASE {DB_NAME} DEFAULT CHARACTER SET 'utf8'")
    except mysql.connector.Error as err:       
        print(f"Faild to create database {err}")    
        exit(1)


# Create a table named clients with its fields in the database
def create_table_clients(cursor):
    create_clients = '''CREATE TABLE clients (
                client_id INT(40) PRIMARY KEY NOT NULL,
                first_name VARCHAR(40),
                last_name VARCHAR(40),
                address VARCHAR(40),
                phone VARCHAR(30)
                )ENGINE=InnoDB'''

    try:
        print("Creating table clients: ", end='')
        cursor.execute(create_clients)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:      
            print("already exists.")
        else:
            print(err.msg)        
    else:
        print("OK")


# Parsing data from the clients.csv into the table 'planets'
def insert_into_clients(cursor):
    insert_sql = []
    path = os.getcwd()
    path = path + '/clients.csv'
    # open the file and read the planets.csv
    with open(path, 'r', encoding='utf-8') as file:
        data = csv.reader(file)
        next(data)  # Data without the first row which is the fields of the table.
        list_rows = []
        for row in data:
            str_row = ''
            # replace all abnormal values in the data list with ''
            # replace all '' with null and a list with strings containning all values in each row
            row = ['' if i == 'NA' or i == 'none' else i for i in row]
            for word in row:
                if word == '':
                    str_row += 'null, '
                else:
                    str_row += '\"' + word + '\", '
            str_row = str_row[0:len(str_row)-2]
            list_rows.append(str_row)
    # All type-converted values in each row in a list of insert_sql
    for row in list_rows:
        query = f'INSERT INTO clients(client_id, first_name, last_name, address, phone)VALUES({row});'
        insert_sql.append(query)

    # excute each query in the list of insert_sql built above
    for query in insert_sql:
        try:          
            cursor.execute(query)
        except mysql.connector.Error as err:
            print(err.msg)
        else:
            # Make sure data is committed to the database
            cnx.commit()
                                             
                
# Create a table named species with its fields in the database
def create_table_invoices(cursor):
    create_invoices = '''CREATE TABLE invoices (
                invoice_id INT(40) PRIMARY KEY AUTO_INCREMENT,
                reference_no BIGINT(60),
                client_id INT(40),
                invoice_total INT(20),
                invoice_date VARCHAR(60),
                due_date VARCHAR(60),
                payment_date VARCHAR(60),
                payment_method VARCHAR(60)
                )ENGINE=InnoDB'''

    try:
        print("Creating table invoices: ", end='')
        cursor.execute(create_invoices)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:      
            print("already exists.")
        else:
            print(err.msg)        
    else:
        print("OK")


# Parsing data from the invoices.csv into the table 'species'
def insert_into_invoices(cursor):
    insert_sql = []
    path = os.getcwd()
    path2 = path + '/invoices.csv'
    # open the file and read the planets.csv
    with open(path2, 'r', encoding='utf-8') as file:
        data = csv.reader(file)
        next(data)    # Data without the first row which is the fields of the table.                                                          # 有时间这里可以改一下
        list_rows = []
        for row in data:
            str_row = ''
            # replace all abnormal values in the data list with ''
            # replace all '' with null and a list with strings containning all values in each row
            row = ['' if i == 'NA' or i == 'none' else i for i in row]           
            for word in row:
                if word == '':
                    str_row += 'null, '
                else:
                    str_row += '\"' + word + '\", '
            str_row = str_row[0: len(str_row)-2]
            list_rows.append(str_row)
    # All type-converted values in each row in a list of insert_sql
    for row in list_rows:
        query = f'INSERT INTO invoices(invoice_id, reference_no, client_id, invoice_total, invoice_date, due_date, '\
                f'payment_date, payment_method)VALUES({row});'
        insert_sql.append(query)
    # excute each query in the list of insert_sql built above
    for query in insert_sql:
        try:            
            cursor.execute(query)
        except mysql.connector.Error as err:
            print(err.msg)
        else:
            #Data committed to the database            
            cnx.commit()                                    


def create_table_paymentMethods(cursor):
    create_paymentMethods = '''CREATE TABLE paymentMethods (
                payment_method_id INT(40) PRIMARY KEY NOT NULL,
                method VARCHAR(40)
                )ENGINE=InnoDB'''

    try:
        print("Creating table paymentMethods: ", end='')
        cursor.execute(create_paymentMethods)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:      
            print("already exists.")
        else:
            print(err.msg)        
    else:
        print("OK")


# Parsing data from the paymentMethods.csv into the table 'species'
def insert_into_paymentMethods(cursor):
    insert_sql = []
    path = os.getcwd()
    path2 = path + '/paymentMethods.csv'
    # open the file and read the planets.csv
    with open(path2, 'r', encoding='utf-8') as file:
        data = csv.reader(file)
        next(data)    # Data without the first row which is the fields of the table.                                                          # 有时间这里可以改一下
        list_rows = []
        for row in data:
            str_row = ''
            # replace all abnormal values in the data list with ''
            # replace all '' with null and a list with strings containning all values in each row
            row = ['' if i == 'NA' or i == 'none' else i for i in row]           
            for word in row:
                if word == '':
                    str_row += 'null, '
                else:
                    str_row += '\"' + word + '\", '
            str_row = str_row[0: len(str_row)-2]
            list_rows.append(str_row)
    # All type-converted values in each row in a list of insert_sql
    for row in list_rows:
        query = f'INSERT INTO paymentMethods(payment_method_id, method)VALUES({row});'
        insert_sql.append(query)
    # excute each query in the list of insert_sql built above
    for query in insert_sql:
        try:            
            cursor.execute(query)
        except mysql.connector.Error as err:
            print(err.msg)
        else:
            #Data committed to the database            
            cnx.commit()


# Define a function of menu that for users to make a choice to make a implementation
def show_main_menu():
    print("Main menu:")
    print("1.Count how many clients who paid the rent.")
    print("2.Count the total rent of each month.")
    print("3.List the payment method of each invoice.")
    print("4.List the name and address of clients who has not pay the rent.")
    print("5.List the phone number that the invoice payment method is swish.")
    print("6.Create a view of with bills only of February and use the view.")
    print("7.Quit")


print()

# Check the database with my first name exsits or not.
try:
    cursor.execute("USE zejian")
    print("Database zejian exists.")
except mysql.connector.Error as err:
    print("Database zejian does not exist.")
    if err.errno == errorcode.ER_BAD_DB_ERROR:                        
        create_database(cursor, 'zejian')
        print("Database zejian created succesfully.")
        cnx.database = 'zejian'  
        create_table_clients(cursor)
        insert_into_clients(cursor)
        create_table_invoices(cursor)
        insert_into_invoices(cursor)
        create_table_paymentMethods(cursor)
        insert_into_paymentMethods(cursor)

# Use while loop to provide users a menu to make a choice infinitely untill the option is 7
option = '0'
while (option != '7'):
    show_main_menu()

    option = input("Make a choice by entering a number and press enter: ")

    # list all planets 
    if option == '1':
        query_1 = '''SELECT COUNT(payment_date)
                     FROM invoices
                     WHERE payment_date IS NOT NULL;'''
        cursor.execute(query_1)
        for (number, ) in cursor:
            print(f"There are {number} clients paid the invoices")
        # Press key to continue showing the menu for users to make a choice.
        key = input('Press enter to continue: ')
        if key.isascii():
            continue

    # Search for planet details
    elif option == '2':
        query_2 = '''SELECT invoice_date, SUM(invoice_total) AS total_rent
                     FROM invoices
                     GROUP BY invoice_date;'''
        cursor.execute(query_2)
        print("| {:<15} | {}".format("invoice_date", "total_rent"))
        print("-"*68)
        for (invoice_date, total_rent) in cursor:
            print("| {:<15} | {}".format(invoice_date, total_rent))
        key = input('Press enter to continue: ')
        if key.isascii():
            continue
    
    # Search for species with height higher than given number
    elif option == '3':
        query_3 = '''SELECT inv.invoice_id, inv.reference_no, pm.method AS payment_method
                     FROM invoices AS inv
                     JOIN paymentmethods AS pm
                     ON inv.payment_method = pm.payment_method_id'''
        cursor.execute(query_3)
        print("| {:<15} | {:<15} | {}".format("invoice_id", "reference_no", "payment_method"))
        print("-"*68)
        for (invoice_id, reference_no, payment_method) in cursor:
            print("| {:<15} | {:<15} | {}".format(invoice_id, reference_no, payment_method))
        key = input('Press enter to continue: ')
        if key.isascii():
            continue

    # Show the most likely desired climate of the given species
    elif option == '4':
        query_4 = '''SELECT clients.first_name AS first_name, clients.last_name AS last_name, clients.address AS address
                     FROM clients
                     JOIN  invoices ON invoices.client_id = clients.client_id
                     WHERE invoices.payment_date IS NULL;'''
        cursor.execute(query_4)
        print("| {:<15} | {:<15} | {}".format("first_name", "last_name", "address"))
        print("-"*68)
        for (first_name, last_name, address) in cursor:
            print("| {:<15} | {:<15} | {}".format(first_name, last_name, address))
        key = input('Press enter to continue: ')
        if key.isascii():
            continue

    # the average lifespan per species classification
    elif option == '5':
        query_5 = '''SELECT cl.phone AS phone
                     FROM clients AS cl
                     JOIN invoices AS inv ON cl.client_id = inv.client_id
                     JOIN paymentMethods AS pm ON inv.payment_method = pm.payment_method_id
                     WHERE pm.method = 'Swish' AND cl.phone IS NOT NULL;'''
        cursor.execute(query_5)
        lst = []
        for (phone, ) in cursor:
            lst.append(phone)
        print(f'The phone number of client who pay the rent by Swish, {lst}')
        key = input('Press enter to continue : ')
        if key.isascii():
            continue
    # quit and stop running the program
    elif option == '6':
        query = '''CREATE VIEW Feb_bills AS 
                   SELECT invoices.reference_no, invoices.invoice_date, invoices.due_date
                   From invoices
                   WHERE invoices.due_date = '2022/2/28';'''
        cursor.execute(query)
        query_6 = '''SELECT reference_no, due_date
                     FROM Feb_bills'''
        cursor.execute(query_6)
        print("| {:<15} | {}".format("reference_no", "due_date"))
        print("-"*68)
        for (reference_no, due_date) in cursor:
            print("| {:<15} | {}".format(reference_no, due_date))
        key = input('Press enter to continue: ')
        if key.isascii():
            continue
    elif option == '7':
        sys.exit()

    else:
        key = input('Follow the instruction and press enter to continue : ')
        if key.isascii():
            continue

cursor.close()
cnx.close()