# Phonepe_Pulse_Data_Visualization
The PhonePe Pulse Data Visualization Project aims to extract, process, and visualize data from the PhonePe Pulse GitHub repository. This repository contains a wealth of metrics and statistics that can offer valuable insights and information. The project's primary objective is to create an interactive and visually appealing dashboard that presents these insights in a user-friendly manner. By combining data extraction, transformation, database integration, and advanced visualization techniques, we provide a comprehensive solution for understanding the data within the PhonePe Pulse repository.

# Problem Statement
The PhonePe Pulse GitHub repository contains diverse data sets with valuable information. However, this data can be overwhelming and challenging to interpret without effective visualization tools. Our project addresses this challenge by creating a dynamic dashboard that enables users to explore and comprehend the data more easily. The solution involves data extraction, transformation, database storage, and visualization creation. The ultimate goal is to provide users with an interactive platform to uncover insights and patterns that support decision-making processes.

# Architectural Diagram
![Phonepe_1](https://github.com/sundarganesh77/Phonepe_Pulse_Data_Visualization/assets/113372806/aaec077d-7fbc-4177-ac3d-a0c59f0d7fc8)

## Approach
### 1. Data Extraction
Python libraries like Git, Subprocess is used to clone the PhonePe Pulse GitHub repository, and to retrieve the necessary data. This data will be stored in a suitable format, such as CSV or JSON, for further processing.

### 2. Data Transformation
Using Python and libraries like Pandas, we will preprocess and clean the extracted data. This step involves handling missing values, formatting, and organizing the data for analysis.

### 3. Database Integration
Utilizing the "mysql-connector-python" library to establish a connection with a MySQL database. The transformed data will be inserted into this database, ensuring efficient storage and retrieval.

### 4. Dashboard Creation
By employing the Streamlit and Plotly libraries in Python, An interactive dashboard is created. This dashboard will utilize Plotly's geo mapping capabilities and Streamlit's user-friendly interface to offer an engaging experience for users.

### 5. Data Retrieval and visualization
Using the "mysql-connector-python" library, Data is fetched from the MySQL database into a Pandas dataframe. This data will be used to dynamically update and populate the dashboard. once the data is retrieved dashboard is created and using some sql queries the data is visualized.

### Summary
The culmination of this project will be a live geo visualization dashboard that provides insights and information derived from the PhonePe Pulse GitHub repository. The dashboard's interactive nature will empower users to explore various metrics and statistics with ease. With over 10 different dropdown options available, users can customize their dashboard experience and extract meaningful insights. By maintaining data in a MySQL database, the dashboard remains up-to-date and relevant.

In summary, the PhonePe Pulse Data Visualization Project offers a robust and user-friendly solution for extracting, transforming, and visualizing data. By combining technical expertise with advanced visualization tools, we aim to provide a valuable resource for data analysis and decision-making.
