from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column,DateTime
from sqlalchemy.types import *
from faker import Faker
from ensurepip import bootstrap
import datetime
import json 
import random
from kafka import KafkaProducer


engine = create_engine("postgresql://postgres:864327@localhost:5432/postgres")
session = sessionmaker(bind=engine)()

base = declarative_base()


class Orders_basket_based(base):
    __tablename__ = "order_basket_based"
    __table_args__ = {"extend_existing":True}
    order_id = Column(Integer,primary_key=True,autoincrement=True)
    customer_id = Column(Integer)
    order_date = Column(DateTime)
    total_cost = Column(Integer)
    
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

f = Faker()
i=0
# Email sütunu databasede Unique olarak tanımlıdır. Dolayısıyla hata vermesi durumunda hatayı görmezden gelip döngüye devam ettiriyoruz. 
while True:
    try:
        session.add_all([Customers(email=f.email())])
        i+=1
        if i==100:
            break
    except:
        continue
session.commit()
# Kullanıcı emailleri üretildi. ve customers tablosuna yazıldı


ORDER_KAFKA_TOPIC="order_details"

producer = KafkaProducer(bootstrap_servers="localhost:9092")

total_product_piece = 2 # Her kullanıcının sepetinde 2 ürün siparişi verdiğini belirtiyoruz.

customer = session.query(Customers)

for id, cust in enumerate(customer):
    # Sipariş önce sepet bazında order_basket_based tablosuna ekleniyor.
    # Daha sonra ürün bazında ayrı ayrı order_product_based tablosuna eklenecek. ürün id(product_id) ve sipariş id(order_id) ile birlikte(for döngüsünün içinde)
    us = Orders_basket_based(customer_id=cust.customer_id, order_date=datetime.datetime.now(), total_cost=0)
    session.add_all([us])
    session.commit()
    session.add_all([Email_send(order_id=us.order_id,sended=0)])
    session.commit()
    
    for j in range(1, total_product_piece+1):
        # 1. ve 2. ürün Products tablosundan rastgele olarak seçiliyor. Ve her ürün için ayrı olarak Producera yollanıyor.
        product = session.query(Products).filter(Products.product_id==random.randint(1, 4)).first()
        piece = random.randint(1, 2)
        data={
            "order_id": us.order_id,
            "order_sequence": j,
            "total_product_piece": total_product_piece,
            "customer_id": cust.customer_id,
            "product_id": product.product_id,
            "cost": product.cost,
            "piece": piece,
            "product_name": product.product_name,
            "order_date": str(datetime.datetime.now())
        }
        producer.send(
            ORDER_KAFKA_TOPIC,
            json.dumps(data).encode("utf-8")
        )
        
        print(f"{cust.email} tarafından gelen {piece} adet {product.product_name} siparişi kaydı gönderildi. customer_id: {cust.customer_id}")








