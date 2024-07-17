# CAPSTONE-350
Credit Card System Data Analysis and Visualization
Project Overview
This project is a comprehensive analysis and visualization tool for a credit card system. The project involves loading data into a MySQL database, providing a console-based interface for users to interact with the data, and generating visualizations for business analysis.

Features

Data Loading
Extract data from JSON files.
Transform the data based on specified mapping documents.
Load the transformed data into a MySQL database named creditcard_capstone.

Console-Based Menu
The application includes a console-based menu that allows users to:
Check Account Details: View existing account details of a customer.
Modify Account Details: Update existing account details of a customer.
Generate Monthly Bill: Calculate and display the monthly bill for a credit card.
Display Transactions: Show transactions made by a customer between two dates.

Data Analysis and Visualization
Using Python libraries, the following visualizations are generated:

Transaction Type with Highest Transaction Count: A bar plot showing the count of each transaction type.
Top 10 States with Highest Number of Customers: A horizontal bar plot showing the states with the highest number of customers.
Top 10 Customers with Highest Transaction Amounts: A horizontal bar plot showing the customers with the highest transaction amounts.

Installation
Prerequisites
Python 3.x
MySQL Database
Required Python Libraries: mysql-connector-python, pandas, matplotlib, seaborn

Steps
Clone the repository:


git clone https://github.com/holand360/credit-card-system-analysis.git
cd credit-card-system-analysis
Install the required Python libraries:


pip install mysql-connector-python pandas matplotlib seaborn
Set up the MySQL database:

Create a database named creditcard_capstone.
Load the data into the database as per the ETL scripts provided.
Usage
Running the Console-Based Menu
Execute the Python script for the console-based menu:


python console_menu.py
Follow the on-screen instructions to interact with the customer data.

Generating Visualizations
Execute the Python script for generating visualizations:


python data_analysis.py
The visualizations will be saved as PNG files in the project directory and displayed on the screen.

File Structure

credit-card-system-analysis/
│
├── data/
│   ├── CDW_SAPP_BRANCH.JSON
│   ├── CDW_SAPP_CREDITCARD.JSON
│   └── CDW_SAPP_CUSTOMER.JSON
│
├── etl/
│   ├── extract.py
│   ├── transform.py
│   └── load.py
│
├── console_menu.py
├── data_analysis.py
├── README.md
└── requirements.txt
Contributing
Fork the repository.
Create a new branch (git checkout -b feature-branch).
Commit your changes (git commit -am 'Add new feature').
Push to the branch (git push origin feature-branch).
Create a new Pull Request.
License
This project is licensed under the MIT License - see the LICENSE file for details.

Acknowledgments
Special thanks to the open-source community for providing the tools and libraries used in this project.
Thanks to the business analyst team for their valuable input and requirements.