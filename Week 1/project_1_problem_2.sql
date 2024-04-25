---- Step 1: Get distinct cities and states in the us_cities table 
---- Step 2: Get eligible customers and attach customers' geolocation.
---- Step 3: Get suppliers and attach suppliers' geolocation.
---- Step 4: For each customer, create a record for each supplier location and then determine the shortest distance.
---- Step 5: Start with eligible customers, join onto customer survey table, then join recipe tags onto this table
---- Step 6: Flatten the tag_property field to make a wide table with a column for each of the 3 tags
---- Step 7: Flatten the tag_list field to make a long table with 1 recipe_tag for each recipe, add a row number
---- Step 8: Join the customer table with the first record of the recipe_tag from the recipe tags table

-- Now that we know which customers can order from Virtual Kitchen, 
-- we want to launch an email marketing campaign to let these customers know 
-- that they can order from our website. If the customer completed a survey 
-- about their food interests, then we also want to include up to 
-- three of their choices in a personalized email message.

-- We would like the following information:
-- Customer ID
-- Customer email
-- Customer first name
-- Food preference #1
-- Food preference #2
-- Food preference #3
-- One suggested recipe 

---- Step 1: Get distinct cities and states in the us_cities table 
with distinct_city_state as(
    select distinct 
        lower(trim(city_name)) as city_name, 
        lower(trim(state_abbr)) as state_abbr,
        geo_location
    from vk_data.resources.us_cities),

---- Step 2: Get eligible customers and attach customers' geolocation.
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
    order by cl.last_name, cl.first_name),

-- Create a query to return those customers who are eligible to order 
-- and have at least one food preference selected. Include up to three of their 
-- food preferences. If the customer has more than three food preferences, 
-- then return the first three, sorting in alphabetical order. 

-- Add a column to the query from Step 1 that 
-- suggests one recipe that matches food preference #1.

---- Step 5: Start with eligible customers, join onto customer survey table, then join recipe tags onto this table

customer_preference_tags as (
    select 
        csd.first_name,
        csd.email,
        csd.customer_id,
        rt.tag_property,
        row_number() over (partition by cs.customer_id order by rt.tag_property) as tag_id
    from customers_suppliers_distance as csd
    inner join vk_data.customers.customer_survey as cs
        on csd.customer_id = cs.customer_id 
    inner join vk_data.resources.recipe_tags as rt
        on cs.tag_id = rt.tag_id
        where cs.is_active = 'TRUE'),
---- Step 6: Flatten the tag_property field to make a wide table with a column for each of the 3 tags

customer_flatten_tags as (        
    select
        *
    from customer_preference_tags
    pivot(min(tag_property) for tag_id in (1, 2, 3))
        as p(first_name, email, customer_id, food_pref_1, food_pref_2, food_pref_3)
),
---- Step 7: Flatten the tag_list field to make a long table with 1 recipe_tag for each recipe, add a row number
recipe_tags as(
    select 
        recipe_id,
        recipe_name as suggested_recipe,
        trim(replace(flat_tag.value, '"', '')) as recipe_tag,
        row_number() over (partition by recipe_tag order by recipe_id, recipe_name) as recipe_property_tag_id
    from vk_data.chefs.recipe
    , table(flatten(tag_list)) as flat_tag)
---- Step 8: Join the customer table with the first record of the recipe_tag from the recipe tags table

select
    cft.first_name,
    cft.email,
    cft.customer_id,
    cft.food_pref_1,
    cft.food_pref_2,
    cft.food_pref_3,
    rec_tags.suggested_recipe
from customer_flatten_tags as cft
join recipe_tags as rec_tags
    on lower(trim(cft.food_pref_1)) = lower(trim(rec_tags.recipe_tag))
    where recipe_property_tag_id = 1
    order by email
---- confirmed: returns 1,048 rows.
---- further work:
-- clean up suggested_recipe field for typos
-- add in tests to check final versus raw record counts