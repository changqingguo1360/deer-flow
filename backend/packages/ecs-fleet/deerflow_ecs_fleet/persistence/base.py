from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase


class FleetBase(DeclarativeBase):
    metadata = MetaData()
