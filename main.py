import os
import csv
import re
import mysql.connector
import skimage
from dateutil.parser import parse
import matplotlib.pyplot as plt
import datetime
from skimage.io import imread, imsave


def replfunc(m):
    mm = ''
    dd = ''
    if len(m.group(1)) == 1:
        mm = '0' + m.group(1)
    else:
        mm = m.group(1)
    if len(m.group(2)) == 1:
        dd = '0' + m.group(2)
    else:
        dd = m.group(2)
    return '20' + m.group(3) + '-' + mm + '-' + dd


# Преобразование исходных данных: для заданной страны получить списки зарегистрированных заражений по дням
def transform_data(target, csv_file):
    notebook_path = os.path.abspath("db_lab5.ipynb")
    output_file = os.path.join(os.path.dirname(notebook_path), csv_file)

    with open(output_file, encoding='utf-8') as r_file:
        file_reader = csv.reader(r_file, delimiter=",")
        line1 = r_file.readline()
        line1 = line1.replace('\n', '')
        line1 = re.sub(r'(\d{1,2})/(\d{1,2})/(\d{2})', replfunc, line1)
        dates_arr = line1.split(',')
        del dates_arr[0:4]

        dates_list = []

        for date in dates_arr:
            dt = parse(date)
            dates_list.append(dt.date())
        infected_list = []
        infected_list.extend([0, ] * (len(dates_list) - len(infected_list)))
        for row in file_reader:
            if row[1] == target:
                del row[0:4]
                row = [int(item) for item in row]
                infected_list = list(map(sum, zip(infected_list, row)))

    return dates_list, infected_list


def get_countries(csv_file):
    countries_list = []
    countries_arr = []
    notebook_path = os.path.abspath("db_lab5.ipynb")
    output_file = os.path.join(os.path.dirname(notebook_path), csv_file)

    with open(output_file, encoding='utf-8') as r_file:
        file_reader = csv.reader(r_file, delimiter=",")
        for row in file_reader:
            countries_arr.append(row[1])

        uniq_set = set(countries_arr)
        for el in uniq_set:
            countries_list.append(el)

        countries_list.sort()
    print("countries count: "+str(len(countries_list)))
    return countries_list


def create_and_fill_tables(db_name, countries_list):
    mydb = mysql.connector.connect(host='localhost', user='root', password='root')
    cursor = mydb.cursor(buffered=True)
    cursor.execute('USE {}'.format(db_name))

    cursor.execute("DROP TABLE IF EXISTS graph")
    cursor.execute("DROP TABLE IF EXISTS infection")
    cursor.execute("DROP TABLE IF EXISTS country")

    mydb.commit()

    TABLES = {}

    TABLES['country'] = (
        "CREATE TABLE IF NOT EXISTS `country` ("
        "  `id` INT NOT NULL AUTO_INCREMENT,"
        "  `name` varchar(50) NOT NULL,"
        "  PRIMARY KEY (`id`)"
        ") ENGINE=InnoDB")

    TABLES['infection'] = (
        "CREATE TABLE IF NOT EXISTS `infection` ("
        "  `id` INT NOT NULL, "
        "  `date` DATE NOT NULL, "
        "  `infected` INT, "
        "  `Rt` FLOAT DEFAULT 0.0, "
        "  FOREIGN KEY (`id`)  REFERENCES country (`id`)"
        ") ENGINE=InnoDB")

    TABLES['graph'] = (
        "CREATE TABLE IF NOT EXISTS graph ("
        "  `id` INT NOT NULL,"
        "   `country` VARCHAR(50) NOT NULL, "
        "  `date` DATE NOT NULL, "
        "  `plot` BLOB,"
        "  FOREIGN KEY (`id`)  REFERENCES country (`id`) "
        ") ENGINE=InnoDB")

    add_country = ("INSERT INTO country "
                   "(name) "
                   "VALUE (%s)")

    add_record = ("INSERT INTO infection "
                  "(id, date, infected) "
                  "VALUES (%s, %s, %s)")

    find_index = ("SELECT id FROM country WHERE name = %s")

    for table in TABLES:
        table_description = TABLES[table]
        cursor.execute(table_description)  # создали таблицы

    for el in countries_list:
        cursor.execute(add_country, (el,))

    mydb.commit()

    for el in countries_list:
        dates_list, infected_list = transform_data(el, 'time_series_covid19_confirmed_global.csv')
        for j in range(len(infected_list)):
            cursor.execute(find_index, [el, ])
            inde = cursor.fetchone()
            inde = inde[0]
            # print('inde= {}'.format(inde))
            # print(list(cursor)[0][0])
            cursor.execute(add_record, [inde, dates_list[j], infected_list[j]])

    mydb.commit()

    #cursor.execute('SELECT id FROM COUNTRY WHERE name = %s', ['Russia', ])
    #russia = cursor.fetchone()
    #russia = russia[0]
    #cursor.execute('SELECT * FROM infection WHERE id = %s', [russia, ])
    #tab = cursor.fetchall()
    #for str in tab:
        #print(str)

    mydb.close()


def get_rt_data(db_name,country_name):
    mydb = mysql.connector.connect(host='localhost', user='root', password='root')
    cursor = mydb.cursor(buffered=True)
    cursor.execute('USE {}'.format(db_name))

    # Функции LAG и LEAD
    select_all =('Select infection.id,country.name,infection.date,infection.infected,infection.Rt'
                 ' From infection JOIN country ON country.id = infection.id WHERE country.name =%s')

    get_rt = (
        "SELECT infection.id,infection.date, "
        "LAG(infection.infected,4,0) OVER(ORDER BY infection.date), "
        "LAG(infection.infected,3,0) OVER(ORDER BY infection.date), "
        "LAG(infection.infected,2,0) OVER(ORDER BY infection.date), "
        "LAG(infection.infected,1,0) OVER(ORDER BY infection.date), "
        "LEAD(infection.infected,1,0) OVER(ORDER BY infection.date), "
        "LEAD(infection.infected,2,0) OVER(ORDER BY infection.date), "
        "LEAD(infection.infected,3,0) OVER(ORDER BY infection.date), "
        "LEAD(infection.infected,4,0) OVER(ORDER BY infection.date) "
        "FROM infection "
        "JOIN country ON country.id = infection.id "
        #"WHERE country.name = %s AND infection.date = %s")
        "WHERE country.name = %s")

    insert_rt = (
        "UPDATE infection SET infection.Rt = %s "
        "WHERE infection.date = %s"
    )

    cursor.execute(get_rt,(country_name,))
    tab = cursor.fetchall()
    for el in tab:
        if (el[2]+el[3]+el[4]+el[5])!=0:
            cursor.execute(insert_rt,((el[6]+el[7]+el[8]+el[9])/(el[2]+el[3]+el[4]+el[5]),el[1]))

    mydb.commit()

    cursor.execute(select_all,(country_name,))
    tab2 = cursor.fetchall()
    #for str in tab2:
        #print(str)
    print(f'Now completed rt for {country_name}')


    mydb.close()


def plot_data(db_name,country):
    mydb = mysql.connector.connect(host='localhost', user='root', password='root')
    cursor = mydb.cursor(buffered=True)
    cursor.execute('USE {}'.format(db_name))


    get_index=("SELECT id FROM country WHERE name = %s")

    get_data =("SELECT date, Rt from infection WHERE id = %s")

    get_graph=("SELECT * FROM graph")

    cursor.execute(get_index, [country, ])
    idx = cursor.fetchone()
    idx = idx[0]
    cursor.execute(get_data,[idx,])
    tab = cursor.fetchall()

    arr_date =[]
    arr_rt = []
    for el in tab:
        arr_date.append(el[0])
        arr_rt.append(el[1])

    cursor.execute('SELECT * FROM graph WHERE id =%s', (idx,))
    tab0 = cursor.fetchone()
    if tab0 ==None:
        fig,ax = plt.subplots(nrows =1,ncols=1,figsize = (10,5))
        ax.set_xlabel('Dates')
        ax.set_ylabel('Rt')
        plt.title('Covid19 Rt cofficient')
        plt.plot(arr_date,arr_rt,label =country)
        plt.legend(loc="upper right")

    ###Здесь должен быть блок сохранения графика в mysql###
        fig.savefig(f"{country}.jpg")
        f = open(f'{country}.jpg', 'rb')
        binary_graph = f.read()
        f.close()

        cursor.execute(get_graph)
        tab1 =cursor.fetchall()
        for el2 in tab1:
            print(el2)

        cursor.execute('SELECT * FROM graph WHERE country =%s',(country,))
        tab = cursor.fetchall()
        if tab==[]:
            print('tab0 = none')
            cursor.execute('INSERT INTO graph (id,country,date,plot) VALUES (%s,%s,%s,%s)',(idx,country,datetime.datetime.now(),binary_graph))
            mydb.commit()


    ###Конец блока сохранения###
    else:
        with open(f'{country}.jpg', 'wb') as file:
            file.write(tab0[3])
            file.close()

        img = skimage.io.imread(f'{country}.jpg')
        fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(10, 5))
        print('here')
        plt.axis('off')
        ax.imshow(img)


        #plt.imshow(m)


    mydb.close()
    return arr_date,arr_rt

# def read_graph(...):
# Чтение графических данных из БД и запись их в файл

# TODO


def plot_all(db_name,countries_str):

    countries = countries_str.split(',')
    countries_dates = []
    countries_rt = []
    #countries_names = []
    for el in countries:
        get_rt_data(db_name,el)
        d,rt = plot_data(db_name,el)
        countries_dates.append(d)
        countries_rt.append(rt)
        #countries_names.append(el)

    if len(countries) > 1:
        fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(10, 5))
        ax.set_xlabel('Dates')
        ax.set_ylabel('Rt')
        plt.title('Covid19 Rt cofficient')
        for el in countries:
            plt.plot(countries_dates[countries.index(el)], countries_rt[countries.index(el)], label=el)

        plt.legend(loc="upper right")

    plt.show()
# plt.figure()

# TODO

# plt.show()


if __name__ == "__main__":

    DB_NAME = 'lab5base'
    update = input('Do you want to update data from file and recreate db tables?\nEnter \"y\" or \"n\":')
    if update =='y':
        countries_list = get_countries('time_series_covid19_confirmed_global.csv')
        mydb = mysql.connector.connect(host='localhost', user='root', password='root')
        c = mydb.cursor(buffered=True)
        c.execute("CREATE DATABASE IF NOT EXISTS {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
        mydb.commit()
        c.close()
        create_and_fill_tables(DB_NAME,countries_list)
        mydb.close()
        print('All tables recreated.')

    name = ""
    while(name != 'q'):
        name = input("(Enter \'q\' for exit)\nEnter one or more countries with delimiter \',\' to see Rt graph: ")
        if(name =='q'):
            continue
        else:
            #get_rt_data(DB_NAME,name)
            #plot_data(DB_NAME,name)
            plot_all(DB_NAME,name)


    # plot_data(...)
    # read_graph(...)
# plot_all(...)
