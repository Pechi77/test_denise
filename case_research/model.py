from sqlalchemy import (Column, Date, ForeignKeyConstraint, Integer, Numeric,
                        PrimaryKeyConstraint, String, Text)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class CaseInfo(Base):
    __tablename__ = "case_info"

    # case information
    citation_number = Column(String(128), primary_key=True)
    filling_date = Column(String(128))
    violation_county = Column(String(128))
    case_status = Column(String(128))

    # defendant information
    name = Column(String(128))
    address = Column(String(128))
    city = Column(String(128))
    state = Column(String(128))
    zip_code = Column(String(128))

    # charge and disposition information information
    charge_description = Column(Text)
    fine_amount_owed = Column(Numeric)
    
    # additional information
    scraped_time = Column(String(128), primary_key=True)
    link = Column(String(1024))