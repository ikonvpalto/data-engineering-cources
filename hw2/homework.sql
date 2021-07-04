-- вывести количество фильмов в каждой категории, отсортировать по убыванию.

select count(*) as films_count, c."name" as category_name from film_category fc 
join category c on fc.category_id = c.category_id
group by c.category_id 
order by films_count desc;

-- вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.

select a.first_name, a.last_name, sum(r.return_date  - r.rental_date) as total_rental_duration from film_actor fa 
join actor a on a.actor_id = fa.actor_id 
join inventory i on i.film_id = fa.film_id
join rental r on i.inventory_id = r.inventory_id 
group by a.actor_id
order by total_rental_duration desc
limit 10;

-- вывести категорию фильмов, на которую потратили больше всего денег.

select c."name" as category_name, sum(f.rental_duration * f.rental_rate) as rental_sum from film_category fc  
join category c on c.category_id = fc.category_id 
join film f on f.film_id = fc.film_id
group by c.category_id 
order by rental_sum desc
limit 1;

-- вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.

select f.title from film f
left join inventory i on f.film_id = i.film_id 
where i.film_id IS NULL;

-- вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.

select a.first_name, a.last_name, count(fa.film_id) as films_count from film_actor fa
join actor a on a.actor_id = fa.actor_id 
join film_category fc on fa.film_id = fc.category_id 
join category c on fc.category_id = c.category_id 
where c."name" = 'Children'
group by a.actor_id, a.first_name, a.last_name 
having count(fa.film_id) >= (
    select count(fa.film_id) as films_count from film_actor fa
    join film_category fc on fa.film_id = fc.category_id 
    join category c on fc.category_id = c.category_id 
    where c."name" = 'Children'
    group by fa.actor_id
    order by films_count desc 
    offset 2
    limit 1)
order by films_count desc;

-- вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). Отсортировать по количеству неактивных клиентов по убыванию.

select ct.city, sum(c.active) as active_customers_count, sum(1 - c.active) as inactive_customers_count from customer c
join address a on c.address_id = a.address_id
join city ct on ct.city_id = a.city_id 
group by ct.city_id
order by inactive_customers_count desc;

-- вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city), и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.

select * from (
    select c."name" as category_name, sum(EXTRACT(epoch FROM r.return_date - r.rental_date)/3600)::int as total_rental_hours, 'starts with A' as city_kind from film_category fc 
    inner join category c on fc.category_id = c.category_id 
    inner join inventory i on i.film_id = fc.film_id 
    inner join rental r on i.inventory_id = r.inventory_id 
    inner join customer cs on cs.customer_id = r.customer_id 
    inner join address a on a.address_id = cs.address_id 
    inner join city ct on ct.city_id = a.city_id and ct.city like 'A%'
    group by c.category_id
    order by total_rental_hours desc
    limit 1) as result
union
select * from (
    select c."name" as category_name, sum(EXTRACT(epoch FROM r.return_date - r.rental_date)/3600)::int as total_rental_hours, 'has dash' as city_kind from film_category fc 
    inner join category c on fc.category_id = c.category_id 
    inner join inventory i on i.film_id = fc.film_id 
    inner join rental r on i.inventory_id = r.inventory_id 
    inner join customer cs on cs.customer_id = r.customer_id 
    inner join address a on a.address_id = cs.address_id 
    inner join city ct on ct.city_id = a.city_id and ct.city like '%-%'
    group by c.category_id
    order by total_rental_hours desc
    limit 1)  as result;