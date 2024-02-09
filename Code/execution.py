from Code.scd1 import SCD1
from Code.scd2 import SCD2


if __name__ == '__main__':
    while True:
        user_input = int(input('!!! ETL AUDIT PROJECT !!!\n'
                               '1. SCD TYPE 1\n'
                               '2. SCD TYPE 2\n'
                               '3. EXIT\n'
                               'Enter your input_data in numbers (1/2) : '))
        if user_input == 1:
            SCD1().main()
        elif user_input == 2:
            SCD2().main()
        elif user_input == 3:
            exit()
        else:
            print('Invalid input_data !!!')
