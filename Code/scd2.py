from uuid import uuid4
import data as d
import os
import re
from datetime import datetime
import pandas as pd
import mysql.connector
import shutil


class SCD2:
    def __init__(self):
        self.connection = mysql.connector.connect(
            host=d.host,
            database=d.database2,
            user=d.user,
            password=d.password
        )
        self.cursor = self.connection.cursor()
        self.process_all = 'no'

        self.cursor.execute('show tables')
        self.table_names = [table.lower() for tables in self.cursor.fetchall() for table in tables]

    def reset(self):
        reset_input = input("Do you want to reset the process? (yes/no): ")
        if reset_input.lower() != 'yes':
            exit()
        else:
            for table in self.table_names:
                self.cursor.execute(f'TRUNCATE TABLE {table}')

            for file_ in os.listdir(d.dest_path_scd2):
                if file_.startswith('Customer_Incr') or file_.startswith('Provider_Incr'):
                    shutil.move(os.path.join(d.dest_path_scd2, file_), d.incr_folder_path)
                elif file_.startswith('Customer') or file_.startswith('Provider'):
                    shutil.move(os.path.join(d.dest_path_scd2, file_), d.fr_folder_path)

            print('DONE!!!\n')

    def load_stats(self):
        self.cursor.execute("INSERT INTO load_stats(load_ctl_key) SELECT load_ctl_key FROM LOAD_CTL WHERE load_ctl_key = (SELECT MAX(load_ctl_key) FROM LOAD_CTL)")
        self.connection.commit()

    def incr_load_stats(self):
        self.cursor.execute("INSERT INTO incr_load_stats(load_ctl_key) SELECT load_ctl_key FROM INCR_LOAD_CTL WHERE load_ctl_key = (SELECT MIN(load_ctl_key) FROM INCR_LOAD_CTL WHERE loaded_ind = 'N')")
        self.connection.commit()

    def lndng_to_stgng(self):
        query = '''
                Insert into staging(Cust_id,Cust_name,Cust_Address,Cust_bn_cd,Cust_SSN,Cust_cvrg_EFFT_date,Cust_cvrg_term_date,Cust_Email,
                Cust_PH_NUm,create_ts,load_ctl_key,Prov_id,Prov_name,Prov_address,Cust_key,checksum,trans_cd, row_id)
                Select R.Cust_id,R.Cust_name,R.Cust_Address,R.Cust_bn_cd,R.Cust_SSN,R.Cust_cvrg_EFFT_date,R.Cust_cvrg_term_date,R.Cust_Email,
                R.Cust_PH_NUm,R.create_ts,R.load_ctl_key,R.Prov_id,R.Prov_name,R.Prov_address,R.Cust_key,R.checksum,R.trans_cd,R.row_id
                FROM 
                (Select s.Cust_id, s.Cust_name, s.Cust_Address, s.Cust_bn_cd, s.Cust_SSN, s.Cust_cvrg_EFFT_date, s.Cust_cvrg_term_date,
                s.Cust_Email, s.Cust_PH_NUm, s.create_ts, s.load_ctl_key, s.Prov_id, s.Prov_name, s.Prov_address,s.Cust_key,s.checksum,
                case
                    when t.Cust_key is null then 'I'
                    when t.Cust_key = s.Cust_key and t.checksum <> s.checksum then 'U'
                    Else 'X'
                end as trans_cd,
                row_number() over(partition by cust_id order by s.load_ctl_key) as row_id
                FROM
                (Select C.Cust_id, C.Cust_name, C.Cust_Address, C.Cust_bn_cd, C.Cust_SSN, C.Cust_cvrg_EFFT_date, C.Cust_cvrg_term_date,
                C.Cust_Email, C.Cust_PH_NUm, C.create_ts, C.load_Ctl_key, P.Prov_id, P.Prov_name, P.Prov_address,md5(C.Cust_id) AS Cust_key,
                md5(CONCAT(C.Cust_name, C.Cust_Address, C.Cust_cvrg_EFFT_date, C.Cust_Email, C.Cust_PH_Num , P.Prov_name, P.Prov_address)) AS checksum
                FROM Customer C INNER JOIN Provider P on C.Cust_id = P.Cust_id WHERE c.load_ctl_key = (SELECT max(load_ctl_key) FROM load_ctl)
                ) AS s
                LEFT JOIN target_eligibility t ON t.cust_id = s.cust_id and t.flag = 'Y') as R
                where R.row_id = 1               
                '''
        self.cursor.execute(query)
        self.cursor.execute("Update target_eligibility SET flag = 'N', update_ts = date_sub(create_ts, INTERVAL 1 second) where cust_key in (Select cust_key from staging where trans_cd = 'U') and flag = 'Y'")
        self.connection.commit()

    def incr_lndng_to_stgng(self):
        query = '''
                Insert into incr_staging(Cust_id,Cust_name,Cust_Address,Cust_bn_cd,Cust_SSN,Cust_cvrg_EFFT_date,Cust_cvrg_term_date,Cust_Email,
                Cust_PH_NUM,Prov_id,Prov_name,Prov_address,load_ctl_key,create_ts,Action_Indicator,Cust_key, Checksum, trans_cd, row_id)
                Select R.Cust_id,R.Cust_name,R.Cust_Address,R.Cust_bn_cd,R.Cust_SSN,R.Cust_cvrg_EFFT_date,R.Cust_cvrg_term_date,R.Cust_Email,
                R.Cust_PH_NUM,R.Prov_id,R.Prov_name,R.Prov_address,R.load_ctl_key,R.create_ts,R.Action_Indicator,R.Cust_key,R.Checksum, R.trans_cd, R.row_id
                From
                (Select s.Cust_id, s.Cust_name, s.Cust_Address, s.Cust_bn_cd, s.Cust_SSN, s.Cust_cvrg_EFFT_date, s.Cust_cvrg_term_date,
                s.Cust_Email, s.Cust_PH_NUm,s.Prov_id, s.Prov_name, s.Prov_address,s.load_ctl_key, s.create_ts,s.Action_Indicator,s.Cust_key,s.checksum,
                case
                    when s.Action_Indicator = 'D' then 'D'
                    when t.Cust_key is null then 'I'
                    when t.Cust_key = s.Cust_key and t.checksum <> s.checksum then 'U'
                    Else 'X'
                end as trans_cd,
                row_number() over(partition by cust_id order by s.load_ctl_key) as row_id                
                FROM 
                (Select C.Cust_id, C.Cust_name, C.Cust_Address, C.Cust_bn_cd, C.Cust_SSN, C.Cust_cvrg_EFFT_date, C.Cust_cvrg_term_date,
                C.Cust_Email, C.Cust_PH_NUm, C.create_ts, C.load_Ctl_key, P.Prov_id, P.Prov_name, P.Prov_address,C.Action_Indicator,md5(C.Cust_id) AS Cust_key,
                md5(CONCAT(C.Cust_name, C.Cust_Address, C.Cust_cvrg_EFFT_date, C.Cust_Email, C.Cust_PH_Num , P.Prov_name, P.Prov_address)) AS checksum
                FROM Incr_Customer C INNER JOIN Incr_Provider P on C.Cust_id = P.Cust_id WHERE c.load_ctl_key = (SELECT min(load_ctl_key) FROM incr_load_ctl where loaded_ind = 'N')
                ) AS s
                LEFT JOIN INCR_ELIGIBILITY t ON t.cust_id = s.cust_id and t.flag = 'Y') as R
                where R.row_id = 1
                '''
        self.cursor.execute(query)
        self.cursor.execute("delete e.* from incr_eligibility e join incr_staging s on e.cust_id = s.cust_id where s.trans_cd = 'D'")
        self.cursor.execute("Update incr_eligibility SET flag = 'N', update_ts = date_sub(create_ts, INTERVAL 1 second) where cust_id in (Select cust_id from incr_staging where action_indicator = 'U' and flag = 'Y')")
        self.connection.commit()

    def stgng_to_base(self):
        query = '''
                    Insert into TARGET_ELIGIBILITY(Cust_id,Cust_name,Cust_Address,Cust_bn_cd,Cust_SSN,Cust_cvrg_EFFT_date,Cust_cvrg_term_date,Cust_Email,
                    Cust_PH_NUm,create_ts,load_Ctl_key,Prov_id,Prov_name,Prov_address,Cust_key,checksum)
                    Select s.Cust_id, s.Cust_name, s.Cust_Address, s.Cust_bn_cd, s.Cust_SSN, s.Cust_cvrg_EFFT_date, s.Cust_cvrg_term_date,
                    s.Cust_Email, s.Cust_PH_NUm, s.create_ts, s.load_Ctl_key, s.Prov_id, s.Prov_name, s.Prov_address, s.Cust_key, s.checksum
                    FROM staging s where trans_cd != 'X';
                '''
        self.cursor.execute(query)
        self.cursor.execute("update TARGET_ELIGIBILITY set flag = 'N', update_ts = current_timestamp() where cust_key not in (select cust_key from staging) and flag = 'Y'")
        self.cursor.execute(
            f" UPDATE load_ctl SET loaded_ind = 'Y', publish_ts = current_timestamp() WHERE load_ctl_key = (SELECT MAX(load_ctl_key) FROM load_stats)")
        self.cursor.execute("UPDATE load_stats SET create_ts = current_time(), status='C' WHERE load_ctl_key = (SELECT MAX(load_ctl_key) FROM Load_ctl)")
        self.inserted_records()
        self.cursor.execute('TRUNCATE staging')
        self.connection.commit()

    def incr_stgng_to_base(self):
        query = '''
                    Insert into incr_eligibility(Cust_id,Cust_name,Cust_Address,Cust_bn_cd,Cust_SSN,Cust_cvrg_EFFT_date,Cust_cvrg_term_date,Cust_Email,
                    Cust_PH_NUm,create_ts,load_Ctl_key,Prov_id,Prov_name,Prov_address,Cust_key,checksum)
                    Select s.Cust_id, s.Cust_name, s.Cust_Address, s.Cust_bn_cd, s.Cust_SSN, s.Cust_cvrg_EFFT_date, s.Cust_cvrg_term_date,
                    s.Cust_Email, s.Cust_PH_NUm, s.create_ts, s.load_Ctl_key, s.Prov_id, s.Prov_name, s.Prov_address, s.Cust_key, s.checksum
                    FROM incr_staging s where trans_cd != 'X'
                '''
        self.cursor.execute(query)
        self.cursor.execute("Update incr_eligibility SET flag = 'N', update_ts = current_timestamp() where cust_id in (Select cust_id from incr_staging where action_indicator = 'D') and flag = 'Y'")
        self.cursor.execute(
            "UPDATE incr_load_ctl SET loaded_ind = 'Y', publish_ts = current_timestamp() WHERE load_ctl_key = (SELECT load_ctl_key FROM incr_staging group by load_ctl_key)")
        self.cursor.execute("UPDATE incr_load_stats SET create_ts = current_timestamp(), status='C' WHERE load_ctl_key = (SELECT load_ctl_key FROM incr_staging group by load_ctl_key)")
        self.inserted_incr_records()
        self.cursor.execute('TRUNCATE incr_staging')
        self.connection.commit()

    def inserted_records(self):
        if self.process_all.lower() != 'yes':
            self.cursor.execute("Select e.* from target_eligibility e join staging s on e.cust_key = s.cust_key where s.trans_cd = 'I'")
            new = len(self.cursor.fetchall())
            print('The number of record NEW RECORD inserted to target table - ', new)
            self.cursor.execute("Select e.* from target_eligibility e join staging s on e.cust_key = s.cust_key where s.trans_cd = 'U'")
            updated = len(self.cursor.fetchall())
            print('The number of record UPDATED RECORD inserted to target table - ', updated)
            self.cursor.execute('Select * from target_eligibility')
            print('The total number of record inserted to target table - ', len(self.cursor.fetchall()))

    def inserted_incr_records(self):
        if self.process_all.lower() != 'yes':
            self.cursor.execute("Select e.* from incr_eligibility e join incr_staging s on e.cust_id = s.cust_id where s.action_indicator = 'I'")
            new = len(self.cursor.fetchall())
            print('The number of record NEW RECORD inserted to target table - ', new)
            self.cursor.execute("Select e.* from incr_eligibility e join incr_staging s on e.cust_id = s.cust_id where s.action_indicator = 'U'")
            updated = len(self.cursor.fetchall())
            print('The number of record UPDATED RECORD inserted to target table - ', updated)
            self.cursor.execute(
                "Select e.* from incr_eligibility e join incr_staging s on e.cust_id = s.cust_id where s.trans_cd = 'D'")
            non_active = len(self.cursor.fetchall())
            print('The number of record NON ACTIVE RECORD inserted to target table - ', non_active)
            self.cursor.execute('Select * from incr_eligibility')
            print('The total number of record inserted to target table - ', len(self.cursor.fetchall()))

    def return_date(self, file):
        dt = lambda x: datetime.strptime(re.findall('\d+_\d+_\d+', x)[0], '%d_%m_%Y')
        date = dt(file).date()
        return date

    def insert_customer(self, current_file_index, cust_files, date_uuid, check):
        if current_file_index < len(cust_files):
            file = cust_files[current_file_index]
            if file not in check:
                cust_file_path = os.path.join(d.fr_folder_path, file)
                df = pd.read_csv(cust_file_path)
                df['load_ctl_key'] = date_uuid

                for index, row in df.iterrows():
                    sql = f'INSERT INTO customer ({d.cust_columns}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                    self.cursor.execute(sql, tuple(row))

                shutil.move(cust_file_path, d.dest_path_scd2)
                self.connection.commit()
                if self.process_all.lower() != 'yes':
                    print(f"Records from {file} inserted and moved to Archives folder successfully!")
            else:
                print(f'The records from file "{file}" is already processed!!!')

    def insert_provider(self, current_file_index, prov_files, date_uuid, check):
        if current_file_index < len(prov_files):
            file = prov_files[current_file_index]
            if file not in check:
                prov_file_path = os.path.join(d.fr_folder_path, file)
                df = pd.read_csv(prov_file_path)

                df['load_ctl_key'] = date_uuid

                for index, row in df.iterrows():
                    sql = f'INSERT INTO provider ({d.prov_columns}) VALUES (%s, %s, %s, %s, %s)'
                    self.cursor.execute(sql, tuple(row))

                shutil.move(prov_file_path, d.dest_path_scd2)
                self.connection.commit()
                if self.process_all.lower() != 'yes':
                    print(f"Records from {file} inserted and moved to Archives folder successfully!")
            else:
                print(f'The records from file "{file}" is already processed!!!')

    def insert_incr_customer(self, current_file_index, cust_files, date_uuid, check):
        if current_file_index < len(cust_files):
            file = cust_files[current_file_index]
            if file not in check:
                cust_file_path = os.path.join(d.incr_folder_path, file)
                df = pd.read_csv(cust_file_path)

                df['load_ctl_key'] = date_uuid

                for index, row in df.iterrows():
                    sql = f'INSERT INTO incr_customer ({d.cust_incr_columns}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                    self.cursor.execute(sql, tuple(row))
                shutil.move(cust_file_path, d.dest_path_scd2)
                self.connection.commit()
                if self.process_all.lower() != 'yes':
                    print(f"Records from {file} inserted and moved to Archives folder successfully!")
            else:
                print(f'The records from file "{file}" is already processed!!!')

    def insert_incr_provider(self, current_file_index, prov_files, date_uuid, check):
        if current_file_index < len(prov_files):
            file = prov_files[current_file_index]
            if file not in check:
                prov_file_path = os.path.join(d.incr_folder_path, file)
                df = pd.read_csv(prov_file_path)

                df['load_ctl_key'] = date_uuid

                for index, row in df.iterrows():
                    sql = f'INSERT INTO incr_provider ({d.prov_incr_columns}) VALUES (%s, %s, %s, %s, %s)'
                    self.cursor.execute(sql, tuple(row))

                shutil.move(prov_file_path, d.dest_path_scd2)
                self.connection.commit()
                if self.process_all.lower() != 'yes':
                    print(f"Records from {file} inserted and moved to Archives folder successfully!")
            else:
                print(f'The records from file "{file}" is already processed!!!')

    def execution(self, select_folder, keyword, user_input):
        file_list = sorted(
            [file for file in os.listdir(select_folder) if file.endswith('.csv') and file.startswith(tuple(keyword))],
            key=lambda x: datetime.strptime(re.findall('\d+_\d+_\d+', x)[0], '%d_%m_%Y'))

        formatted_integer = int(datetime.now().strftime('%Y%m%d%H%M%S'))
        unique_id_int = uuid4().int
        lck = int(str(formatted_integer) + str(unique_id_int)[:5])

        date_uuid_map = {}
        for file in file_list:
            dt = lambda x: datetime.strptime(re.findall('\d+_\d+_\d+', x)[0], '%d_%m_%Y')
            date = dt(file).date()

            if date in date_uuid_map:
                continue
            else:
                lck += 1
                date_uuid_map[date] = lck

        file_count = len(date_uuid_map)
        check_existing = os.listdir(d.dest_path_scd2)

        cust_files = [name for name in file_list if name.startswith('Customer')]
        prov_files = [name for name in file_list if name.startswith('Provider')]

        if file_count:
            if user_input == 2:
                self.process_all = input("Do you want to process all the next files together? (yes/no): ")

            current_file_index = 0
            while current_file_index < file_count:
                if len(cust_files) > len(prov_files):
                    file_date = self.return_date(cust_files[current_file_index])
                else:
                    file_date = self.return_date(prov_files[current_file_index])

                if user_input == 1:
                    load_ctl = f'INSERT INTO load_ctl ({d.load_ctl}) VALUES (%s)'
                    self.cursor.execute(load_ctl, (date_uuid_map[file_date],))
                    self.connection.commit()
                    self.insert_customer(current_file_index, cust_files, date_uuid_map[file_date], check_existing)
                    self.insert_provider(current_file_index, prov_files, date_uuid_map[file_date], check_existing)
                    self.load_stats()
                    permission = input("Do you want load to the staging table? (yes/no): ")
                    if permission.lower() != 'yes':
                        print('\n Abort!!!\n\n')
                    else:
                        self.lndng_to_stgng()
                        permission = input("Do you want load to the base table? (yes/no): ")
                        if permission.lower() != 'yes':
                            print('\n Abort!!!\n\n')
                        else:
                            self.stgng_to_base()

                if user_input == 2:
                    load_ctl = f'INSERT INTO incr_load_ctl ({d.load_ctl}) VALUES (%s)'
                    self.cursor.execute(load_ctl, (date_uuid_map[file_date],))
                    self.connection.commit()
                    self.insert_incr_customer(current_file_index, cust_files, date_uuid_map[file_date], check_existing)
                    self.insert_incr_provider(current_file_index, prov_files, date_uuid_map[file_date], check_existing)
                    self.incr_load_stats()
                    if self.process_all.lower() != 'yes':
                        permission = input("Do you want load to the staging table? (yes/no): ")
                        if permission.lower() != 'yes':
                            print('\n Abort!!!\n\n')
                        else:
                            self.incr_lndng_to_stgng()
                            permission = input("Do you want load to the base table? (yes/no): ")
                            if permission.lower() != 'yes':
                                print('\n Abort!!!\n\n')
                            else:
                                self.incr_stgng_to_base()
                    else:
                        self.incr_lndng_to_stgng()
                        self.incr_stgng_to_base()

                current_file_index += 1
                if current_file_index < file_count and self.process_all.lower() != 'yes':
                    user = input("\nDo you want to process the next file? (yes/no): ")
                    if user.lower() != 'yes':
                        break
                elif current_file_index == file_count:
                    print('\nAll Files Processed Successfully !!! \n\n')
        else:
            print("\nAll file processed already !!!\n")

    def main(self):
        with open(d.rqm_table, 'r') as words:
            word = [row.strip('\n') for row in words]
        invalid = [key for key in word if key.lower() not in self.table_names]
        check_file = os.path.getsize(d.rqm_table)

        if check_file == 0:
            print(f'The file {d.rqm_table} is empty')
        elif invalid:
            print('The table name is wrong - ', *invalid, sep='\n')
        else:
            while True:
                choice = int(input("Choose one of them :\n"
                                   "1. Fully Refresh files. \n"
                                   "2. Incremental files. \n"
                                   "3. Reset and truncate the table. \n"
                                   "4. Exit. \n"
                                   "5. Main Menu. \n"
                                   "Your choice = "))

                if choice == 1:
                    self.execution(d.fr_folder_path, word, choice)
                elif choice == 2:
                    self.execution(d.incr_folder_path, word, choice)
                elif choice == 3:
                    self.reset()
                elif choice == 4:
                    exit()
                elif choice == 5:
                    print('\n\n\n')
                    break
                else:
                    print('\n Invalid choice \n\n')

        self.cursor.close()
        self.connection.close()
