from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column
from sqlalchemy.types import *
from faker import Faker
import time 
import random

from ensurepip import bootstrap
import imp
import json

from kafka import KafkaConsumer
from kafka import KafkaProducer

ORDER_KAFKA_TOPIC="order_details"
ORDER_CONFIRMED_KAFKA_TOPIC="order_confirmed"


consumer=KafkaConsumer(
    ORDER_KAFKA_TOPIC,
    bootstrap_servers="localhost:9092"
)
producer=KafkaProducer(
    bootstrap_servers="localhost:9092"
)




engine = create_engine("postgresql://postgres:864327@localhost:5432/postgres")
session = sessionmaker(bind=engine)()

base = declarative_base()

class Orders_basket_based(base):
    __tablename__ = "order_basket_based"
    __table_args__ = {"extend_existing":True}
    order_id = Column(Integer,primary_key=True,autoincrement=True)
    customer_id = Column(Integer)
    order_date = Column(Date)
    total_cost = Column(Integer)
    
class Orders_product_based(base):
    __tablename__ = "order_product_based"
    __table_args__ = {"extend_existing":True}
    id=Column(Integer,primary_key=True,autoincrement=True)
    order_id = Column(Integer)
    product_id = Column(Integer)
    piece = Column(Integer)

class Products(base):
    __tablename__ = "product"
    __table_args__ = {"extend_existing":True}
    product_id = Column(Integer,primary_key=True)
    product_name = Column(VARCHAR)
    cost = Column(Integer)

class Customers(base):
    __tablename__ = "customers"
    __table_args__ = {"extend_existing":True}
    customer_id = Column(Integer,primary_key=True)
    email = Column(VARCHAR)

class Email_send(base):
    __tablename__ = "email_send"
    __table_args__ = {"extend_existing":True}
    order_id = Column(Integer,primary_key=True)
    sended = Column(Boolean)
base.metadata.create_all(engine)


print("Gonna start listening")
while True:
    for message in consumer:
        print("Ongoing transactions")
        consumed_message=json.loads(message.value.decode())
        
        order_id = consumed_message["order_id"]
        order_sequence = consumed_message["order_sequence"]
        total_product_piece = consumed_message["total_product_piece"]
        customer_id = consumed_message["customer_id"]
        product_id = consumed_message["product_id"]
        product_name = consumed_message["product_name"]
        cost = consumed_message["cost"]
        piece = consumed_message["piece"]
        order_date = consumed_message["order_date"]
        
        # Sepetteki her ürünü ayrı ayrı order_product_based tablosuna işliyoruz
        session.add_all([Orders_product_based(order_id=order_id,product_id=product_id,piece=piece)])
        
        # Sepetteki toplam ürün tutarını ana sipariş tablomuz olan order_basket_based tablosunun total_cost sütununa ekliyoruz.
        basket = session.query(Orders_basket_based).filter(Orders_basket_based.order_id==order_id).first()
        basket.total_cost += cost*piece
        
        session.commit()
        print(f"{piece} adet {product_name} sipariş kaydı alındı. Order id:{order_id} {order_date}")
        
        # Sepette birden fazla ürün olduğunda sepetteki ürün miktarıyla yeni gelen kaydın sepetteki sırasının eşit olup olmadığını
        # kontrol ediyoruz. Eşitse, artık sepetteki tüm ürünler hem sepet bazında hem ürün bazında veritabanına kaydedildiği için producera mail talimatı gönderiyoruz.
        if order_sequence == total_product_piece:
            customer_email = session.query(Customers).filter(Customers.customer_id==customer_id).first()
    
            data={
                "order_id":order_id,
                "customer_email":customer_email.email
            }
    
            producer.send(
                ORDER_CONFIRMED_KAFKA_TOPIC,
                json.dumps(data).encode("utf-8")
            )



