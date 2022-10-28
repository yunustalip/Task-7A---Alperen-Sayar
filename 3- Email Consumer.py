from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column
from sqlalchemy.types import *
from ensurepip import bootstrap
from kafka import KafkaAdminClient, KafkaConsumer
import json

engine = create_engine("postgresql://postgres:864327@localhost:5432/postgres")
session = sessionmaker(bind=engine)()

base = declarative_base()
base.metadata.create_all(engine)


class Email_send(base):
    __tablename__ = "email_send"
    __table_args__ = {"extend_existing":True}
    order_id = Column(Integer,primary_key=True)
    sended = Column(Boolean)

ORDER_KAFKA_CONFIRMED_TOPIC="order_confirmed"

consumer=KafkaConsumer(
    ORDER_KAFKA_CONFIRMED_TOPIC,
    bootstrap_servers="localhost:9092"
)

print("Email is listening")

while True:
    for message in consumer:
        consumed_message=json.loads(message.value.decode())
        order_id=consumed_message["order_id"]
        customer_email=consumed_message["customer_email"]

        
        basket = session.query(Email_send).filter(Email_send.order_id==order_id).first()
        if basket.sended==0:
            print(f"{customer_email} adresine mail gönderiliyor...")
            basket.sended=1
            session.commit()
            print(f"{customer_email} adresine mail gönderildi.")


