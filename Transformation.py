import pandas as pd

def run_transformation():

    # Remove Duplicates
    data = pd.read_csv(r'C:\Users\Oaikhena\Downloads\10alytics Data Engineering Course\Zipco_foods_ETL_Airflow_For_Orchestration\zipco_transaction.csv')
    data.drop_duplicates(inplace = True)

    # Handle missing numeric values by filling with the mean or median
    numeric_columns = data.select_dtypes(include = ['float64', 'int64']).columns
    for col in numeric_columns:
        data.fillna({col : data[col].mean()}, inplace = True)

    # Handle missing string values by filling with 'Unknown'
    string_columns = data.select_dtypes(include = ['object']).columns
    for col in string_columns:
        data.fillna({col : 'Unknown'}, inplace = True)

    # Assigning the right datatype to the date column
    data['Date'] = pd.to_datetime(data['Date'])

    # Creating the dimension tables

    # Create the product table
    products = data[['ProductName']].copy().drop_duplicates().reset_index(drop = True)
    products.index.name = 'ProductID'
    products = products.reset_index()

    # Create the customer table
    customers = data[['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail']].copy().drop_duplicates().reset_index(drop = True)
    customers.index.name = 'CustomerID'
    customers = customers.reset_index()


    # Create the staff table
    staff = data[['Staff_Name', 'Staff_Email', 'StaffPerformanceRating']].copy().drop_duplicates().reset_index(drop = True)
    staff.index.name = 'StaffID'
    staff = staff.reset_index()

    # Creating the fact table

    # Create Transactions table
    transactions = data.merge(products, on = ['ProductName'], how = 'left')\
        .merge(customers, on = ['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail'], how = 'left')\
            .merge(staff, on = ['Staff_Name', 'Staff_Email', 'StaffPerformanceRating'], how = 'left')

    transactions = transactions[['Date', 'ProductID', 'UnitPrice', 'Quantity', 'StoreLocation',
       'PaymentType', 'PromotionApplied', 'Weather', 'Temperature', 'CustomerFeedback', 'DeliveryTime_min',
       'OrderType', 'CustomerID', 'StaffID', 'DayOfWeek', 'TotalSales']]

    transactions.index.name = 'Transaction_ID'
    transactions = transactions.reset_index()

    # save cleaned data as csv files
    data.to_csv('clean_data.csv', index = False)
    products.to_csv('products.csv', index = False)
    customers.to_csv('customers.csv', index = False)
    staff.to_csv('staff.csv', index = False)
    transactions.to_csv('transactions.csv', index = False)

    print('Data cleaning and transformation completed successfully')