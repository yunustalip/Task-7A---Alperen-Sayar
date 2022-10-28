CREATE TABLE public.order_product_based (
	id serial4 NOT NULL,
	order_id int4 NULL,
	product_id int4 NULL,
	piece int4 NULL,
	CONSTRAINT order_product_based_pk PRIMARY KEY (id),
	CONSTRAINT order_product_based_fk FOREIGN KEY (order_id) REFERENCES public.order_basket_based(order_id),
	CONSTRAINT order_product_based_fk2 FOREIGN KEY (product_id) REFERENCES public.product(product_id)
);


CREATE TABLE public.order_basket_based (
	order_id int4 NOT NULL DEFAULT nextval('order_basket_based_id_seq'::regclass),
	customer_id int4 NULL,
	total_cost int4 NULL DEFAULT 0,
	total_product_piece int4 NULL,
	order_date date NULL,
	CONSTRAINT order_basket_based_pkey PRIMARY KEY (order_id),
	CONSTRAINT order_basket_based_fk FOREIGN KEY (customer_id) REFERENCES public.customers(customer_id)
);

CREATE TABLE public.product (
	product_id int4 NOT NULL,
	product_name varchar NULL,
	"cost" int4 NULL,
	CONSTRAINT product_pk PRIMARY KEY (product_id)
);
CREATE INDEX product_product_id_idx ON public.product USING btree (product_id);



CREATE TABLE public.customers (
	customer_id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
	email varchar NULL,
	CONSTRAINT customers_un UNIQUE (email),
	CONSTRAINT users_pkey PRIMARY KEY (customer_id)
);


CREATE TABLE public.email_send (
	order_id int4 NOT NULL,
	sended bool NULL,
	CONSTRAINT email_send_pk PRIMARY KEY (order_id),
	CONSTRAINT email_send_fk FOREIGN KEY (order_id) REFERENCES public.order_basket_based(order_id)
);
CREATE INDEX email_send_order_id_idx ON public.email_send USING btree (order_id);