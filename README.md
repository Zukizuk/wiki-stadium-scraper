# Wikipedia Football Stadiums Scraper & ETL Pipeline

## 📌 Overview

This project extracts data from [Wikipedia](https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity), orchestrates the process using Apache Airflow, loads the data into PostgreSQL, and visualizes it with Power BI.

## 🚀 Features

- **Web Scraping:** Extracts football stadium data from Wikipedia using `BeautifulSoup`.
- **ETL Orchestration:** Apache Airflow manages the extraction, transformation, and loading process.
- **Database Storage:** Stores raw and normalized data in PostgreSQL.
- **Data Validation:** Ensures data consistency with row count checks.
- **Power BI Integration:** Connects PostgreSQL to Power BI for visualization.

## 🛠️ Tech Stack

- **Python** (requests, BeautifulSoup, Airflow)
- **PostgreSQL** (data storage and normalization)
- **Apache Airflow** (ETL pipeline orchestration)
- **Power BI** (data visualization)

## 📂 Project Structure

```

📁 project-root/
│── 🐍 scrape.py # Scrapes stadium data from Wikipedia
│── 🐍 db_utils.py # Handles PostgreSQL operations (table creation, insertion, validation)
│── 🐍 scraping_utils.py # Helper functions for extracting and cleaning data
│── 🐍 wiki-scraper_dag.py # Defines the Airflow DAG
│── 📄 README.md # Project documentation

```

## 📌 Data Pipeline Workflow

1. **Extract** → Scrapes Wikipedia using `scrape.py`.
2. **Transform** → Cleans and structures data.
3. **Load** → Inserts data into PostgreSQL.
4. **Validate** → Ensures row count consistency.
5. **Visualize** → Connects PostgreSQL to Power BI.

## 🛠️ Setup & Installation

### 1️⃣ Clone the Repository

```bash
git clone https://github.com/Zukizuk/wiki-stadium-scraper
cd wiki-stadium-scraper
```

### 2️⃣ Create a Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`
```

### 3️⃣ Install Dependencies

if you haven't already installed airflow refer to this link to download and install airflow with docker (recommended) [Install Airflow](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)

### 5️⃣ Start Airflow

Once you have installed airflow with docker, start the container:
Then, open `http://localhost:8080` in your browser.

### 6️⃣ Trigger the DAG

In the Airflow UI, find `wikipedia` with the tag.

## 🎯 Power BI Integration

1. Connect to PostgreSQL from Power BI.
2. Load the `labs.raw_stadium_data`.
3. Create your visualization

## 🤝 Contributing

Contributions are welcome! Feel free to open issues or submit pull requests.

## 📜 License

This project is licensed under the MIT License.

---

🔗 **Author:** Zuki  
📧 Contact: [sannimarzuk@gmail.com](mailto:sannimarzuk@gmail.com)
