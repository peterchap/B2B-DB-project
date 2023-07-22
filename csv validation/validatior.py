import pandas as pd
import pandas_schema
from pandas_schema import Column
from pandas_schema.validation import CustomElementValidation, InListValidation, MatchesPatternValidation
import numpy as np
from decimal import *

def check_email:
print(df[~df.email.str.contains("@",na=False)])
df = df[df.email.str.contains("@",na=False)]
char = '\+|\*|\'| |\%|,|\"|\/'
df = df[~df['email'].str.contains(char,regex=True)]

def check_decimal(dec):
    try:
        Decimal(dec)
    except InvalidOperation:
        return False
    return True


def check_int(num):
    try:
        int(num)
    except ValueError:
        return False
    return True

null_validation = [CustomElementValidation(lambda d: d is not np.nan, 'this field cannot be null')]

def do_validation():
    # read the data
    data = pd.read_csv('data.csv')

    # define validation elements
    decimal_validation = [CustomElementValidation(lambda d: check_decimal(d), 'is not decimal')]
    int_validation = [CustomElementValidation(lambda i: check_int(i), 'is not integer')]
    null_validation = [CustomElementValidation(lambda d: d is not np.nan, 'this field cannot be null')]

    class EmailValidation(_SeriesValidation):
        """ 
        Checks if email address is valid format
        """
        def __init__(self, email : str, **kwargs):
            self.email = email
            super().__init__(**kwargs)
        
        @property
        def default_message(self):
            return 'invalid email format "{}"'.format(self.email)

        def valid_email(self, email):
            char = '\+|\*|\'| |\%|,|\"|\/'
            try:
                df = df[df.email.str.contains("@",na=False)]

                df = df[~df['email'].str.contains(char,regex=True)]
                datetime.datetime.strptime(address, self.email)
                return True
                
            except:
                return False

        def validate(self, series: pd.Series) -> pd.Series:
            return series.astype(str).apply(self.valid_email)

    # define validation schema
    schema = pandas_schema.Schema([
            Column(MatchesPatternValidation(r'^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w{2,3}$') ,\
                null_validation()]),
            Column('firstname', decimal_validation),
            Column('gender', decimal_validation),
            Column('lastname', decimal_validation),
            Column('leaddate', decimal_validation),
            Column('leadurl', decimal_validation),
            Column('leadipaddress', decimal_validation),
            Column('phonenumber', int_validation + null_validation),
            Column('mobile', MatchesPatternValidation(r'^(07[\d]{8,12}|447[\d]{7,11})$') ,\
                null_validation()),
            Column('postalzip', int_validation + null_validation),
            Column('provincestatename', null_validation)
            ])



    # apply validation
    errors = schema.validate(data)
    errors_index_rows = [e.row for e in errors]
    data_clean = data.drop(index=errors_index_rows)

    # save data
    pd.DataFrame({'col':errors}).to_csv('errors.csv')
    data_clean.to_csv('clean_data.csv')


if __name__ == '__main__':
    do_validation()