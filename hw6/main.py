from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType, LongType


def get_rental_duration(return_date, rental_date):
    return f.datediff(return_date, rental_date)


def calc_rental_cost(rental_duration, rental_rate):
    return rental_duration * rental_rate


def invert_active_state(active):
    return 1 - active


rental_duration_func = f.udf(get_rental_duration, IntegerType())
rental_cost_func = f.udf(calc_rental_cost, IntegerType())
invert_active_func = f.udf(invert_active_state, IntegerType())

spark = SparkSession \
    .builder \
    .master('local') \
    .appName("lesson_12") \
    .getOrCreate()


def read_table(table_name):
    db_url = 'enter your url here'

    db_properties = {
        'username': 'postgres',
        'password': 'secret',
        'url': db_url,
        'driver': 'org.postgresql.Driver',
    }

    return spark.read.jdbc(url=db_url, table=table_name, properties=db_properties)


category = read_table('category')
film = read_table('film')
film_category = read_table('film_category')
actor = read_table('actor')
film_actor = read_table('film_actor')
inventory = read_table('inventory')
rental = read_table('rental')
address = read_table('address')
city = read_table('city')
customer = read_table('customer')

category \
    .join(film_category, category.category_id == film_category.category_id) \
    .groupBy(category.category_id) \
    .count() \
    .sort(f.desc('count')) \
    .select('count', category.name.alias('category_name')) \
    .show()

film_actor \
    .join(actor, film_actor.actor_id == actor.actor_id) \
    .join(inventory, inventory.film_id == film_actor.film_id) \
    .join(rental, rental.inventory_id == inventory.inventory_id) \
    .withColumn('rental_duration', rental_duration_func(f.col('return_date'), f.col('rental_date'))) \
    .groupBy(actor.actor_id) \
    .agg(f.sum('rental_duration').alias('total_rental_duration')) \
    .select(actor.first_name, actor.last_name, f.col('total_rental_duration')) \
    .show()

film_category \
    .join(category, category.category_id == film_category.category_id) \
    .join(film, film.film_id == film_category.film_id) \
    .withColumn('rental_cost', rental_cost_func(film.rental_duration, film.rental_rate)) \
    .groupBy(category.category_id) \
    .agg(f.sum('rental_cost').alias('rental_sum')) \
    .sort(f.desc('rental_sum')) \
    .select(category.name.alias('category_name'), f.col('category_name')) \
    .limit(1) \
    .show()

film \
    .join(inventory, inventory.film_id == film.film_id, 'left') \
    .where(inventory.film_id is None) \
    .select(film.title) \
    .show()

third_most_popular_children_actor = film_actor\
    .join(film_category, film_category.film_id == film_actor.film_id) \
    .join(category, film_category.category_id == category.category_id) \
    .where(category.name == 'Children') \
    .groupBy(film_actor.actor_id) \
    .agg(f.count().alias('films_count')) \
    .sort(f.desc('films_count')) \
    .select(f.col('films_count')) \
    .limit(3) \
    .collect()[2] \
    .get(0)

film_actor \
    .join(actor, actor.actor_id == film_actor.actor_id) \
    .join(film_category, film_category.film_id == film_actor.film_id) \
    .join(category, film_category.category_id == category.category_id) \
    .where(category.name == 'Children') \
    .groupBy([actor.actor_id, actor.first_name, actor.last_name]) \
    .agg(f.count().alias('films_count')) \
    .where(f.col('films_count') >= third_most_popular_children_actor) \
    .select(actor.first_name, actor.last_name, f.col('films_count')) \
    .show()

customer \
    .join(address, address.address_id == customer.address_id) \
    .join(city, city.city_id == address.city_id) \
    .withColumn('inactive', invert_active_func(customer.active)) \
    .groupBy(city.city_id) \
    .agg(f.sum(customer.active).alias('active_customers_count')) \
    .agg(f.sum('inactive').alias('inactive_customers_count')) \
    .sort(f.desc('inactive_customers_count')) \
    .select(city.city, f.col('active_customers_count'), f.col('inactive_customers_count')) \
    .show()

film_category \
    .join(category, film_category.category_id == category.category_id) \
    .join(inventory, inventory.film_id == film_category.film_id) \
    .join(rental, rental.inventory_id == inventory.inventory_id) \
    .join(customer, customer.customer_id == rental.customer_id) \
    .join(address, address.address_id == customer.address_id) \
    .join(city, city.city_id == address.city_id) \
    .where(f.col('city').like('A%')) \
    .withColumn('city_kind', 'starts with A') \
    .withColumn('rental_hours', (rental.return_date.case(LongType) - rental.rental_date.case(LongType)) / 3600) \
    .groupBy(category.category_id) \
    .agg(f.sum('rental_hours').alias('total_rental_hours')) \
    .sort(f.desc('total_rental_hours')) \
    .select(category.name.alias('category_name'), f.col('total_rental_hours'),
            f.lit('starts with A').alias('city_kind')) \
    .limit(1) \
    .union(
        film_category
        .join(category, film_category.category_id == category.category_id)
        .join(inventory, inventory.film_id == film_category.film_id)
        .join(rental, rental.inventory_id == inventory.inventory_id)
        .join(customer, customer.customer_id == rental.customer_id)
        .join(address, address.address_id == customer.address_id)
        .join(city, city.city_id == address.city_id)
        .where(f.col('city').like('A%'))
        .withColumn('city_kind', 'starts with A')
        .withColumn('rental_hours', (rental.return_date.case(LongType) - rental.rental_date.case(LongType)) / 3600)
        .groupBy(category.category_id)
        .agg(f.sum('rental_hours').alias('total_rental_hours'))
        .sort(f.desc('total_rental_hours'))
        .select(category.name.alias('category_name'), f.col('total_rental_hours'),
                f.lit('starts with A').alias('city_kind'))
        .limit(1)
    )\
    .show()
