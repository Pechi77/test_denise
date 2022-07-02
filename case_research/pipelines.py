# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

import logging
from datetime import datetime, date

import numpy as np
import pandas as pd
from scrapy.exceptions import DropItem
from scrapy.exporters import BaseItemExporter
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from pandas.tseries.offsets import BDay

from case_research.model import Base, CaseInfo


class MySQLExporter(BaseItemExporter):
    """Item exporter for handling sqlite export"""

    def __init__(self, db, username, password, host, port, **kwargs):
        self.db = db  # filename for the database
        self.scraped_time = datetime.now().strftime("%d/%m/%Y %H:%M")
        self.logger = logging.getLogger("MySQLExporter")
        # connect to mysql server for use with sqlalchemy
        db_uri = f"mysql+pymysql://{username}:{password}@{host}:{port}"
        self.engine = create_engine(db_uri)
        # create database if it doesn't exist already
        with self.engine.connect() as conn:
            conn.execute(f"CREATE DATABASE IF NOT EXISTS {self.db}")
        # connect to database
        db_uri = f"mysql+pymysql://{username}:{password}@{host}:{port}/{self.db}"
        self.engine = create_engine(db_uri)
        self.session_maker = sessionmaker(bind=self.engine)
        # create tables
        Base.metadata.create_all(self.engine, checkfirst=True)

    def _insert_item(self, session, item):
        try:
            session.add(item)
            session.commit()
        except SQLAlchemyError as e:
            self.logger.error("Error entering items into database: %s", e)
            session.rollback()

    def export_item(self, item):
        # make a session for inserting an item
        session = self.session_maker()
        # prepare items for database entry
        case_info_item = CaseInfo(
            citation_number=item["citation_number"],
            filling_date=item["filling_date"],
            violation_county=item["violation_county"],
            case_status=item["case_status"],
            name=item["name"],
            address=item["address"],
            city=item["city"],
            state=item["state"],
            zip_code=item["zip_code"],
            charge_description=item["charge_description"],
            fine_amount_owed=item["fine_amount_owed"],
            scraped_time=self.scraped_time,
            link=item["link"],
        )
        self._insert_item(session, case_info_item)
        session.close()


class MySQLPipeline:
    def __init__(self, mysql_db, username, password, host, port):
        self.mysql_db = mysql_db  # sql database name
        self.username = username
        self.password = password
        self.host = host
        self.port = port

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mysql_db=crawler.settings.get("MYSQL_DB"),
            username=crawler.settings.get("MYSQL_USERNAME"),
            password=crawler.settings.get("MYSQL_PASSWORD"),
            host=crawler.settings.get("MYSQL_HOST"),
            port=crawler.settings.get("MYSQL_PORT"),
        )

    def open_spider(self, spider):
        self.exporter = MySQLExporter(
            db=self.mysql_db,
            username=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
        )
        self.scraped_time = self.exporter.scraped_time
        self.exporter.start_exporting()

    def close_spider(self, spider):
        filename = self.get_filename()
        self.export_csv(filename)
        self.exporter.finish_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def get_filename(self):
        today = date.today()
        filename = "traffic_"+"_".join([(today - BDay(i)).strftime("%d-%m-%Y") for i in [3,1]]) +".csv"
        return filename

    def export_csv(self, filename):
        db_uri = f"mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.mysql_db}"

        engine = create_engine(db_uri)
        session_maker = sessionmaker(bind=engine)

        with session_maker() as session:
            # query = session.query(CaseInfo)
            # instead of query select *
            # names =  select name from case_info where scraped_time = self.scrpaed_time
            # query, select * from case_info where name in names,
            # today_names = session.query(CaseInfo.name).filter_by(scraped_time=self.scraped_time).all()
            today_query = session.query(CaseInfo).filter_by(scraped_time=self.scraped_time)
            # today_names = [item.name for item in today_names]
            # query.filter(CaseInfo.name.in_(today_names))
            # query = session.query(CaseInfo).filter(CaseInfo.name.in_(today_names))
            df = pd.read_sql(today_query.statement, today_query.session.bind)
            # df = pd.read_sql(query.statement, query.session.bind)
            # citation_numbers = df["citation_number"].to_list()
            # citation_query = session.query(CaseInfo).filter(CaseInfo.citation_number.in_(citation_numbers))
            # df = pd.read_sql(citation_query.statement, citation_query.session.bind)
            
        # do filtering
        df = df.fillna("")
        # only keep when fine fine_amount_owed is zero
        df = df[df["fine_amount_owed"] != ""]
        df["fine_amount_owed"] = df["fine_amount_owed"].astype(float)
        df = df[np.isclose(df["fine_amount_owed"], 0.0)]
        # only keep active cases
        
        df = df[df["case_status"].isin(["Open", "ACTIVE CASE", 
                    "RESTRICTED CASE (OFFICER-ID INVALID)"])]
        # output only from current session
        # df = df[df["scraped_time"].isin([self.scraped_time])]
        
        first_scraped_map = df.groupby("name")["scraped_time"].first().to_dict()
        agg = {
            "name": "first",
            "address": "first",
            "city": "first",
            "state": "first",
            "zip_code": "first",
            "violation_county": "first",
            "filling_date": "first",
            #! merge rows with this function
            "citation_number": (lambda x: ", ".join(x)),
            "charge_description": (lambda x: ", ".join(x)),
            "case_status": "first",
            "fine_amount_owed": "first",
            "scraped_time": "first", # the current running date
            "link": (lambda x: ", ".join(x)),
        }

        ### handle previous day
        previous_day = open("case_research/previous_day.txt", "r").read().strip()
        print("BEFORE", df.shape)
        df_previous = pd.read_sql(f"SELECT * FROM case_info where scraped_time = '{previous_day}'", con=engine)
        df = df[~df["citation_number"].isin(df_previous["citation_number"])]
        print("AFTER", df.shape)
        # new column - first_scraped_date,  - first scraped date of a particular person
        df = df.groupby("name").aggregate(agg, as_index=False)
        df = df.reset_index(drop=True)
        df["first_scraped_date"] = df["name"].map(first_scraped_map)

        
        df.to_csv(filename, index=False)
        with open("case_research/previous_day.txt", "w") as f:
            f.write(self.scraped_time)
        print(f"Exported to {filename}")
        #trigger to terminate cloud formation.


class CaseResearchPipeline:
    def process_item(self, item, spider):
        if item["fine_amount_owed"]:
            item["fine_amount_owed"] = item["fine_amount_owed"].replace("$", "")
            item["fine_amount_owed"] = float(item["fine_amount_owed"])

        return item
