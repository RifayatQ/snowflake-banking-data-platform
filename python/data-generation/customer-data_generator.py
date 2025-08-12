import pandas as pd 
import numpy as np 
from datetime import datetime, timedelta
import random
from faker import Faker 

fake = Faker('en_CA') # Canadian locale for realistic data

def generate_customers(n=10000):
    customers = []
    for i in range(n):
        customer = {
            'customer_id': f'CUST_{i:06d}',
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=80),
            'address': fake.street_address(),
            'city': fake.city(),
            'province': fake.province_abbr(),
            'postal_code': fake.postalcode(),
            'sin': fake.ssn(),
            'customer_since': fake.date_between(start_date='-10y', end_date='today'),
            'account_status': random.choice(['Active', 'Inactive', 'Suspended'])      
        }
        customers.append(customer)
    return pd.DataFrame(customers)

def generate_transactions(customers_df, n=100000):
    transactions = []
    for i in range(n):
        customer_id = random.choice(customers_df['customer_id'].tolist())
        transaction = {
            'transaction_id': f'TXN_{i:08d}',
            'customer_id': customer_id,
            'account_id': f'ACC_{random.randint(100000, 999999)}',
            'transaction_date': fake.date_time_between(start_date='-2y', end_date='now'),
            'transaction_type': random.choice(['Debit', 'Credit', 'Transfer', 'Payment']),
            'amount': round(random.uniform(-5000, 5000), 2),
            'merchant_category': random.choice(['Grocery', 'Gas', 'Restaurant', 'Retail', 'Healthcare', 'Entertainment']),
            'description': fake.company(),
            'channel': random.choice(['ATM', 'Online', 'Branch', 'Mobile'])
        }
        transactions.append(transaction)
    return pd.DataFrame(transactions)

customers = generate_customers(10000)
transactions = generate_transactions(customers, 100000)

customers.to_csv('customers.csv', index=False)
transactions.to_csv('transactions.csv', index=False)