---- Step 1: Get distinct cities and states in the us_cities table 
---- Step 2: Get eligible customers and attach customers' geolocation.
---- Step 3: Get suppliers and attach suppliers' geolocation.
---- Step 4: For each customer, create a record for each supplier location and then determine the shortest distance.

---- want to return
-- Customer ID
-- Customer first name
-- Customer last name
-- Customer email
-- Supplier ID
-- Supplier name
-- Shipping distance in kilometers or miles (you choose)

-- Step 1: We have 10,000 potential customers who have signed up with Virtual Kitchen. 
-- If the customer is able to order from us, then their city/state will be present in our database. 
-- Create a query in Snowflake that returns all customers that can place an order with Virtual Kitchen.

-- Step 2: We have 10 suppliers in the United States. 
-- Each customer should be fulfilled by the closest distribution center. 
-- Determine which supplier is closest to each customer, and how far the shipment needs to travel to reach the customer. 
-- There are a few different ways to complete this step. 
-- Use the customer's city and state to join to the us_cities resource table. 
-- Do not worry about zip code for this exercise.
-- Order your results by the customer's last name and first name.

---- Step 1: Get distinct cities and states in the us_cities table 
with distinct_city_state as(
    select distinct 
        lower(trim(city_name)) as city_name, 
        lower(trim(state_abbr)) as state_abbr,
        geo_location
    from vk_data.resources.us_cities),

-- Step 2: Get eligible customers and attach customers' geolocation.
customers_location as
    (
    select 
        cd.customer_id,
        cd.first_name,
        cd.last_name,
        cd.email,
        dcs.city_name,
        dcs.state_abbr,
        dcs.geo_location as customer_geo_location
    from vk_data.customers.customer_data as cd
    inner join vk_data.customers.customer_address as ca
        on cd.customer_id = ca.customer_id
    inner join distinct_city_state as dcs
        on lower(trim(ca.customer_city)) = dcs.city_name and lower(trim(ca.customer_state)) = dcs.state_abbr), 
        
---- Step 3: Get suppliers and attach suppliers' geolocation.
suppliers_location as
(
    select 
        si.supplier_id,
        si.supplier_name,
        dcs.city_name,
        dcs.state_abbr,
        dcs.geo_location as supplier_geo_location
    from vk_data.suppliers.supplier_info as si
    left join distinct_city_state as dcs
        on lower(trim(si.supplier_city)) = dcs.city_name and lower(trim(si.supplier_state)) = dcs.state_abbr),
        
---- Step 4: For each customer, create a record for each supplier location and then determine the shortest distance.
customers_suppliers_distance as (
    select
        cl.customer_id,
        cl.first_name,
        cl.last_name,
        cl.email,
        sl.supplier_id,
        sl.supplier_name,
        st_distance(cl.customer_geo_location, sl.supplier_geo_location) / 1609 as distance_to_supplier_miles
    from customers_location as cl
    cross join suppliers_location as sl
    qualify row_number() over (partition by cl.customer_id order by distance_to_supplier_miles) = 1
    order by cl.last_name, cl.first_name
)
select * from customers_suppliers_distance;
-- select count(*) from customers_suppliers_distance;
---- confirmed: returns 2,401 rows.