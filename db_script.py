import pandas
import mysql.connector
from sqlalchemy import create_engine

'''Учетные данные для подключения к базе данных'''
hostname = "localhost"
dbname = "db_script"
uname = "root"
pwd = "446535"

db_script = mysql.connector.connect(
    host=hostname,
    user=uname,
    passwd=pwd,
    database=dbname)

mycursor = db_script.cursor()
mycursor.execute('CREATE DATABASE IF NOT EXISTS db_script')


class Table:
    def __init__(self):
        return

    '''Функция извлечения значения из строки'''
    def get_value_func(self, file_line):
        self.file_line = file_line
        return self.file_line.split('=')[1].split(';')[0].replace('"', "")

    '''Функция сведения строки к списку'''
    def get_list_func(self, item):
        self.item = item
        return list(map(int, self.item.replace('[', '').replace(']', '').split(', ')))

    '''Скрипт чтения файла - получение значений для таблицы second_pd'''
    def script_func(self, *args):
        self.file_name = []
        for i in range(len(args)):
            self.file_name.append((args[i] + '.txt'))
        print('self.file_names: ' + f'{self.file_name}')

        self.second_pd = pandas.DataFrame(
            columns=['number', 'version', 'family', 'Speed', 'Lenght', 'Weight', 'Viscosity'])
        for i in range(len(self.file_name)):
            with open(self.file_name[i], 'r', encoding="UTF-8") as self.file:
                self.file_line = self.file.readline()
                self.listitems = []
                while self.file_line:
                    self.number = i + 1
                    if self.file_line.find('version=') != -1:
                        self.version = self.get_value_func(self.file_line)
                        # print(self.version)
                    elif self.file_line.find('name=') != -1:
                        self.name = self.get_value_func(self.file_line)
                        # print(self.name)
                    elif self.file_line.find('listitems') != -1:
                        self.listitems.append(self.get_value_func(self.file_line))

                    self.file_line = self.file.readline()

            '''Преобразование для значения listitems списка строк в список списков'''
            self.items = []
            for i in range(len(self.listitems)):
                self.items.append(self.get_list_func(self.listitems[i]))
            # print(self.items)

            '''Получение комбинаций listitems'''
            self.listitems_combination = []
            for i in range(len(self.items[0])):
                for j in range(len(self.items[0])):
                    self.listitems_combination.append(
                        [self.items[0][j], self.items[1][j], self.items[2][i], self.items[3][i]])
            # print(self.listitems_combination)

            '''Составление 1ой таблицы'''
            self.first_pd = pandas.DataFrame(columns=["number", "file_name"])
            for i in range(len(args)):
                self.first_pd = self.first_pd.append({"number": i + 1, "file_name": args[i]}, ignore_index=True)
            # print(self.first_pd)

            '''Составление 2ой таблицы'''
            for i in range(len(self.listitems_combination)):
                self.second_pd = self.second_pd.append(
                    {'number': self.number, 'version': self.version, 'family': self.name,
                     'Speed': self.listitems_combination[i][0],
                     'Lenght': self.listitems_combination[i][1], 'Weight': self.listitems_combination[i][2],
                     'Viscosity': self.listitems_combination[i][3]}, ignore_index=True)
        print(self.second_pd)


t = Table()
t.script_func("test_1", "test_2")


'''SQLAlchemy для подключения к базе данных MySQL'''
engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                       .format(host=hostname, db=dbname, user=uname, pw=pwd))

t.first_pd.to_sql('table_first', engine, if_exists='replace', index=False)
t.second_pd.to_sql('table_second', engine, if_exists='replace', index=False)

