documentation = """
# Тестовое Задание

## Задачи

1. Загрузить данные[набор данных о товарах с сайта DNS](https://www.dns-shop.ru/catalog/17a8a69116404e77/myshi/)
2. Написать даг в AirFlow для загрузки данных в PostgreSQL. **Важно**: даг должен запускаться раз в сутки

## Ожидаемые результаты

1. Код AirFlow-дага

## Инструкции
1. Скачать репозиторий https://github.com/puckel/docker-airflow
2. Добавить в ропазиторий Docker или docker compose файл
3. Добавить файл database.env, пример database.env.example
4. Запустить код из локального интерпретатора или из Airflow docker-compose -f docker-compose-LocalExecutor.yml up -d
5. Запустить команду `sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' 4b0cadb71f9a<-ваш контейнер ID от поднятой базы postgres` проверить совпадение для POSTGRES_CONFIG['hostname']
6. Если у вас не подгрузился selenium установить в контейнер можно с помощью команд  docker exec -it <имя_контейнера> /bin/bash
                                                                                     pip install --upgrade selenium
7. Нужно скачать  webdriver.Chrome по ссылке https://chromedriver.chromium.org/downloads и положить в папку dags
"""
from datetime import timedelta

import pandas as pd
import pendulum
import math
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine
from selenium import webdriver

DEFAULT_TASK_ARGS: dict = {
    "owner": "beaver",
    "retries": 3,
    "depends_on_past": False,
    "provide_context": True,
}  # Default task arguments for DAG

TARGET_TABLE_NAME: str = "test_DNS"

# Reading parameters from the environment file
postgres_user = os.environ.get('POSTGRES_USER')
postgres_password = os.environ.get('POSTGRES_PASSWORD')
postgres_db = os.environ.get('POSTGRES_DB')

POSTGRES_CONFIG: dict = {
    "username": postgres_user,
    "password": postgres_password,
    "hostname": "172.18.0.2",
    "port": 5432,
    "database": postgres_db,
}

PANDAS_ENGINE_SQL_: str = create_engine(
    f"postgresql://{POSTGRES_CONFIG['username']}:{POSTGRES_CONFIG['password']}@{POSTGRES_CONFIG['hostname']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"
)

def load_link_products(**kwargs):
    """
    Run load_link_products.
    Getting all product links from category 'myshi'.
    """
    print("Step 1. Getting all product links from category 'myshi'")
    ti = kwargs["ti"]
    # create an instance of the Options class to configure Selenium WebDriver Chrome
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-dev-shm-usage')

    # instantiate WebDriver Chrome
    driver = webdriver.Chrome(options=options)

    # open the DNS Shop page with a list of products
    driver.get("https://www.dns-shop.ru/catalog/17a8a69116404e77/myshi")

    count_products_all = int(driver.find_elements_by_xpath("//span[@class='products-count]").split(' ')[0])
    count_products_page = len(driver.find_elements_by_xpath("//div[@class='catalog-product ui-button-widget ]"))
    count_page = math.ceil(count_products_all / count_products_page)

    products_link = []

    for search_page in range(count_page):
        driver.get(f"https://www.dns-shop.ru/catalog/17a8a69116404e77/myshi/?p={search_page}")

        # get a list of products and go through each
        products = driver.find_elements_by_xpath("//div[@class='catalog-product ui-button-widget']")

        for product in products:
            # get a link to the products
            products_link.append(product.find_element_by_xpath(".//a[@class='catalog-product__name ui-link ui-link_black']").get_attribute("href"))
    print(products_link)
    ti.xcom_push(key="test_task_DNS", value=products_link)


def load_info_products(**kwargs):
    """
    Run load_info_products.
    Getting product parameters (name, code, price, country of origin) from category 'myshi'
    """
    print("Step 2. Getting product parameters (name, code, price, country of origin) from category 'myshi'")
    ti = kwargs["ti"]
    products_link = ti.xcom_pull(key="load_link_products", task_ids=["load_link_products"])[0]
    # create an instance of the Options class to configure Selenium WebDriver Chrome
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-dev-shm-usage')

    #instantiate WebDriver Chrome
    driver = webdriver.Chrome(options=options)

    info_products = []

    # open links from previously received links
    for page_product in products_link:
        driver.get(f"https://www.dns-shop.ru/{page_product}")
        name = driver.find_elements_by_xpath("//div[@class='product-card-top__name']")
        code = int(driver.find_elements_by_xpath("//div[@class='product-card-top__code']").split(' ')[2])
        price = int(driver.find_elements_by_xpath("//div[@class='product-buy__price']"))
        countre = driver.find_elements_by_xpath("//div[@class='product-characteristics__spec-value']")
        info_products.append([name, code, price, countre])
    ti.xcom_push(key="test_task_DNS", value=info_products)

def writing_data(**kwargs):
    """
    Run writing_data.
    Writing product parameters from category 'myshi' to the database
    """
    print("Step 2. Writing product parameters from category 'myshi' to the database")
    ti = kwargs["ti"]
    info_products = ti.xcom_pull(key="test_task_DNS", task_ids=["load_info_products"])[0]
    df_info_products = pd.DataFrame(info_products, columns = ['name','code','price', 'countre'])
    df_info_products.to_sql(name=TARGET_TABLE_NAME, con=PANDAS_ENGINE_SQL_, if_exists="replace", index=False)


with DAG(
        dag_id="test_task_DNS",
        start_date=pendulum.yesterday("UTC"),
        default_args=DEFAULT_TASK_ARGS,
        schedule_interval=timedelta(hours=24),
        catchup=False,
        max_active_runs=1,
        tags=["data_pipeline", "postgresql"],
        doc_md=documentation,
) as dag:
    load_link_products = PythonOperator(task_id="load_link_products", provide_context=True, python_callable=load_link_products)
    load_info_products = PythonOperator(task_id="load_info_products", provide_context=True, python_callable=load_info_products)
    writing_data = PythonOperator(task_id="writing_data", provide_context=True, python_callable=writing_data)

    load_link_products >> load_info_products >> writing_data