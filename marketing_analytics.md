# Case Study: Marketing Analytics
CREDIT: Danny Ma.

You can view his course here: https://www.datawithdanny.com/


# Project Overview
Customer analytics team at DVD Rental Co who have been tasked with generating the necessary data points required to populate specific parts of this first-ever customer email campaign. Imagine that I am a fresher data analyst of the team. My supervisor, who is experience data leader(Mr. Danny), gave a solution plan. I will follow that plan to solve the problem.

# Summarized Insights

# Here is the approach to solve the problem

There are 2 big steps:
* Main report: it contains all steps to solve the problem. At the end, there is final report which contrains information about customers who rented films and recommendations for them.
* Extra insights: there are 11 questions that can be asked by the marketing team.

## Main report

### 1. Create complete dataset

#### Steps:
- Use **INNER JOIN** to merge ```rental```, ```inventory``` ,```film```,```film_category```and```category```.
- **INNER JOIN** and **LEFT JOIN** are them same. I did some tests to see whether there is a difference.

````sql
DROP TABLE IF EXISTS complete_data_table;
CREATE TEMP TABLE complete_data_table  AS(
    Select
        A.customer_id,
        A.rental_id,
        A.rental_date,
        E.name as category_name,
        C.film_id,
        C.title
    From dvd_rentals.rental A 
    INNER JOIN dvd_rentals.inventory B 
        On A.inventory_id = B.inventory_id
    INNER JOIN dvd_rentals.film C 
        On B.film_id = C.film_id
    INNER JOIN dvd_rentals.film_category D 
        On C.film_id = D.film_id
    INNER JOIN dvd_rentals.category E 
        On D.category_id = E.category_id
);
````


### 2. Customer rental counts for each category

#### Steps:
- Use **Count** and **Group By** to answer how many film each customer watched per category.
- Use **Max** to find the latest rented date per each customer. It will be useful in later step where ranking.

````sql
DROP TABLE IF EXISTS category_counts ;
CREATE TEMP TABLE category_counts AS(
    Select
        customer_id,
        category_name,
        COUNT(*) as rental_count, 
        -- for ranking purspose
        MAX(rental_date) as latest_rental_date
    From complete_data_table
    GROUP BY 1,2
);
````

### 3. Total films each customer watched

#### Steps:
- Use **Sum** and **Group By** to answer how many film each customer watched in total.

````sql
DROP TABLE IF EXISTS total_counts ;
CREATE TEMP TABLE total_counts AS(
    Select
        customer_id,
        SUM(rental_count) as total_count
    From category_counts
    GROUP BY 1
);
````

### 4. Top 2 categories for each customer

#### Steps:
- Use **CTE** and **Dense_Rank()** to rank categories based on rental count and latest rental date.
- Select records where categories in the top 2

````sql
DROP TABLE IF EXISTS top_categories ;
CREATE TEMP TABLE top_categories AS
  WITH cte AS(
    Select
        customer_id,
        category_name,
        rental_count,
        DENSE_RANK() OVER(PARTITION BY customer_id ORDER BY rental_count DESC, 
                                                            latest_rental_date) 
        AS ranked_category
    From category_counts
)
    Select *
    From cte
    WHERE ranked_category <=2;
````

### 5. Average rental count per category

#### Steps:
- Use **AVG()** to answer how many did customers watched on each category on average.
- Use **Floor** to get the nearest integer.

````sql
DROP TABLE IF EXISTS average_category_count ;
CREATE TEMP TABLE average_category_count AS(
    Select
        category_name,
        ---round down to nearest int
        FLOOR(AVG(rental_count)) as avg_category_count
    From category_counts
    GROUP BY category_name
);
````

### 6. Average rental count per category

#### Steps:
- Use **PERCENT_RANK()** to calculate relative rank of each row's percentile
- Usse **CASE WHEN** to transform 0 percentile into 1 percentile

````sql
DROP TABLE IF EXISTS top_category_percentile;
CREATE TEMP TABLE top_category_percentile AS
  With cte AS(
    Select
        B.customer_id,
        B.category_name as top_category_name,
        B.ranked_category,
        A.rental_count,
        A.category_name,
        PERCENT_RANK() OVER (
            PARTITION BY A.category_name 
            ORDER BY A.rental_count DESC
          ) AS percentile_value
    From category_counts A 
    LEFT JOIN top_categories B 
        On A.customer_id = B.customer_id
)
      Select
        customer_id,
        category_name,
        rental_count,
        ranked_category,
        CASE
            WHEN ROUND(100*percentile_value) = 0 then 1 
            ELSE ROUND(100*percentile_value)
         END AS percentile
      From cte
      WHERE ranked_category = 1
          AND top_category_name = category_name;
````



### 7. First top category insights table 

#### Steps:
- Purpose of this table: create a table for top 1 category, which will be used later.

````sql
DROP TABLE IF EXISTS first_top_category_insights;
CREATE TEMP TABLE first_top_category_insights AS(
    Select
        A.customer_id,
        A.category_name,
        A.rental_count,
        A.rental_count - B.avg_category_count AS avg_comparision,
        A.percentile
    From top_category_percentile A 
    LEFT JOIN average_category_count B 
        On A.category_name = B.category_name
);
````

### 8. Second top category insights table 

#### Steps:
- Purpose of this table: create a table for top 2 category, which will be used later.

````sql
DROP TABLE IF EXISTS second_category_insights;
CREATE TEMP TABLE second_category_insights AS(
    Select
        A.customer_id,
        A.category_name,
        A.rental_count,
        ROUND(100* A.rental_count/B.total_count::NUMERIC) as percentage_difference
    From top_categories A 
    LEFT JOIN total_counts B 
        On A.customer_id = B.customer_id
    WHERE ranked_category = 2
);
````

### 9. Summarised film count table 


````sql
DROP TABLE IF EXISTS film_counts ;
CREATE TEMP TABLE film_counts AS (
    Select DISTINCT
        film_id,
        title,
        category_name,
        COUNT(*) OVER(PARTITION BY film_id) AS rental_count
    From complete_data_table
);
````

### 10. A previously watched film table


````sql
DROP TABLE IF EXISTS category_film_exclusions ;
CREATE TEMP TABLE category_film_exclusions AS(
    Select DISTINCT
        film_id,
        customer_id
    FROM complete_data_table
);
````


### 11. Perform an anti join from the relevant category films on the exclusions

#### Steps:
- Use **CTE** and **DENSE_RANK()** to rank recommendation film
- Use **ANTI JOIN** to exclude films which is alread watched

````sql
DROP TABLE IF EXISTS category_recommendations;
CREATE TEMP TABLE category_recommendations AS
  With ranked_cte AS(
    Select
        A.customer_id,
        A.category_name,
        A.ranked_category,
        B.film_id,
        B.title,
        B.rental_count,
        DENSE_RANK() OVER(PARTITION  BY customer_id, ranked_category
            ORDER BY B.rental_count DESC, B.title  
        ) AS reco_rank
    From top_categories A 
    INNER JOIN film_counts B 
        On A.category_name = B.category_name 
    WHERE  NOT EXISTS (
          Select 1
          From category_film_exclusions C 
          WHERE A.customer_id = C.customer_id
              AND B.film_id = C.customer_id
    )
)
Select 
  *
From ranked_cte
WHERE reco_rank <=3;
````



### 12. A new base dataset which has a focus on the actor


````sql
DROP TABLE IF EXISTS actor_joint_table;
CREATE TEMP TABLE actor_joint_table AS(
    Select
      A.customer_id,
      A.rental_id,
      A.rental_date,
      C.film_id,
      D.actor_id,
      CONCAT(E.first_name, ' ', E.last_name) AS actor_name,
      C.title
    From
      dvd_rentals.rental A
      INNER JOIN dvd_rentals.inventory B 
            On A.inventory_id = B.inventory_id
      INNER JOIN dvd_rentals.film C 
            On B.film_id = C.film_id
      INNER JOIN dvd_rentals.film_actor D 
            On C.film_id = D.film_id
      INNER JOIN dvd_rentals.actor E 
            On D.actor_id = E.actor_id
);
````


### 13.  Identify the top actor and their respective rental film count

#### Steps:
- Use **CTE** to count number of actors and rank actor based on numbers of film count

````sql
DROP TABLE IF EXISTS top_actor_counts;
CREATE TEMP TABLE top_actor_counts AS
 WITH actor_count AS(
    SELECT
      customer_id,
      actor_id,
      actor_name,
      COUNT(*) AS rental_count
    FROM
      actor_joint_table
    GROUP BY
      customer_id,
      actor_id,
      actor_name
  ),
  ranked_actor AS (
    Select
      actor_count.*,
      DENSE_RANK() OVER(
        PARTITION BY customer_id
        ORDER BY
          rental_count DESC,
          actor_name
      ) AS rank_actor
    FROM
      actor_count
)
    Select
        *
    From ranked_actor
    WHERE rank_actor = 1;
````

### 14. Generate total actor rental counts

#### Steps:
- Use **CTE** to count number of unique films

````sql
DROP TABLE IF EXISTS actor_film_counts;
CREATE TEMP TABLE actor_film_counts AS 
  WITH film_count AS (
    Select
      film_id,
      COUNT(DISTINCT rental_id) AS rental_count
    FROM
      actor_joint_table
    GROUP BY
      film_id
)
    Select DISTINCT
        A.film_id,
        A.actor_id,
        A.title,
        B.rental_count
    From actor_joint_table A
    LEFT JOIN film_count B 
        On A.film_id = B.film_id;
````

### 15. An updated film exclusions table
#### Steps:
- Use **UNION** to merge available films in total and films which are already recommended

````sql
DROP TABLE IF EXISTS actor_film_exclusions;
CREATE TEMP TABLE actor_film_exclusions AS
(
    Select DISTINCT
        customer_id,
        film_id
    From complete_data_table
)
UNION 
(
    Select DISTINCT
        customer_id,
        film_id
    From category_recommendations
);
````

### 16. Identify the 3 valid film recommendations
#### Steps:
- Use **CTE** and **DENSE_RANK()** to rank recommended films featuring favorite actors
- Use **ANTI JOIN** to exclude films which are already recommeneded and not rented

````sql
DROP TABLE IF EXISTS actor_recommendations ;
CREATE TEMP TABLE actor_recommendations AS
  WITH cte AS (
    Select
        A.customer_id,
        A.actor_name,
        A.rental_count,
        B.title,
        B.film_id,
        B.actor_id,
        DENSE_RANK() OVER(PARTITION BY A.customer_id ORDER BY B.rental_count DESC, B.title) as reco_rank
    FROM top_actor_counts A 
    INNER JOIN actor_film_counts B 
       On A.actor_id = B.actor_id
    WHERE NOT EXISTS (
        Select 1
        FROM actor_film_exclusions C 
        WHERE 
              A.customer_id = C.customer_id
            AND 
              B.film_id = C.film_id
))
    Select
        *
    FROM cte
    WHERE reco_rank <=3;
````
   
    
### 17. Final Report
#### Steps:
- Use **CTE** to merge all insight table
- Use **CONCAT** to create insight in string 

````sql
DROP TABLE IF EXISTS report_table;
CREATE TEMP TABLE report_table AS
  WITH first_category_insight AS(
    Select
      customer_id,
      category_name,
      CONCAT('You ','ve watched ' , rental_count,' ', category_name, ' film, 
      that''s ', avg_comparision, ' more than the DVD Rental Co average and puts you in the top ', percentile, '% of ', category_name ) as insight
    From first_top_category_insights
),
   second_category_insight AS(
      Select 
        customer_id,
        category_name,
        CONCAT('You ','ve watched ' , rental_count,' ', category_name, ' films,', 
      ' making up ',percentage_difference ,'% of your entire viewing history' ) as insight
      From second_category_insights
),
  top_actor AS (
      Select
          customer_id,
          actor_name,
          CONCAT('You''ve watched ', rental_count, ' films featuring ', actor_name, '. Here are some other films ', actor_name, 'stars in that might interest you!' ) as insight
      From top_actor_counts
),
    total_category_recommendations AS (
      Select
          customer_id,
          MAX(CASE WHEN ranked_category = 1 AND reco_rank =  1 THEN title END ) AS cat_1_reco_1,
          MAX(CASE WHEN ranked_category = 1 AND reco_rank =  2 THEN title END ) AS cat_1_reco_2,
          MAX(CASE WHEN ranked_category = 1 AND reco_rank =  3 THEN title END ) AS cat_1_reco_3,
          MAX(CASE WHEN ranked_category = 2 AND reco_rank =  1 THEN title END ) AS cat_2_reco_1,
          MAX(CASE WHEN ranked_category = 2 AND reco_rank =  2 THEN title END ) AS cat_2_reco_2,
          MAX(CASE WHEN ranked_category = 2 AND reco_rank =  3 THEN title END ) AS cat_2_reco_3
      From category_recommendations 
      GROUP BY customer_id
),
    total_actor_recommendations AS (
      Select
        customer_id,
        MAX(CASE WHEN reco_rank =  1 THEN title END ) AS actor_reco_1,
        MAX(CASE WHEN reco_rank =  2 THEN title END ) AS actor_reco_2,
        MAX(CASE WHEN reco_rank =  3 THEN title END ) AS actor_reco_3
      From actor_recommendations
      GROUP BY customer_id
),
    final_output AS (
      SELECT
          A.customer_id,
          C.category_name as cat_1,
          A.cat_1_reco_1,
          A.cat_1_reco_2,
          A.cat_1_reco_3,
          D.category_name as cat_2,
          A.cat_2_reco_1,
          A.cat_2_reco_2,
          A.cat_2_reco_3,
          E.actor_name,
          B.actor_reco_1,
          B.actor_reco_2,
          B.actor_reco_3,
          C.insight AS insight_1,
          D.insight AS insight_2,
          E.insight AS insight_actor
      FROM total_category_recommendations A 
      INNER JOIN total_actor_recommendations B 
        ON A.customer_id = B.customer_id
      INNER JOIN first_category_insight C
        ON A.customer_id = C.customer_id
      INNER JOIN second_category_insight D 
        ON A.customer_id = D.customer_id
      INNER JOIN top_actor E 
        ON A.customer_id = E.customer_id
)
    Select *
    FROM final_output;
````


## Questions from marketing team

### 1. Which film title was the most recommended for all customers?

#### Steps:
- Use **Union Join** to combine 2 recommendation tables created

````sql
With cte as
(Select 
    title
From category_recommendations
UNION ALL
Select 
    title
From actor_recommendations
)
  Select
      title,
      COUNT( title) as reco_count
  FROM cte
  GROUP BY title
ORDER BY reco_count DESC
LIMIT 1;
````
| title          | reco_count  |
| ---------------| ----------- |
|JUGGLER HARDLY  | 145         |



### 2. How many customers were included in the email campaign?

#### Steps:
- Use **COUNT DISTINCT** to count how many customers in total 

````sql
SELECT
    COUNT(DISTINCT customer_id) as count_customers
FROM report_table;
````
| count_customers |
| ----------------| 
| 599             |


### 3. Out of all the possible films - what percentage coverage do we have in our recommendations?

#### Steps:
- Use **UNION JOIN** to extract all recommended films
- Use **CROSS JOIN** to calculate percentages

````sql

with cte as
(Select 
    title
From category_recommendations
UNION 
Select 
    title
From actor_recommendations
)
  Select  
      Count(distinct A.title) as film_reco,
      Count(distinct B.title) as film_total,
      ROUND( Count(distinct A.title)::NUMERIC/Count(distinct B.title)::NUMERIC,5) as coverage
  From cte A 
  CROSS JOIN dvd_rentals.film B;
````
| film_reco   | film_total  | coverage   |
| ----------- | ----------- |----------- |
| 250         | 1000        |0.25000     |






### 4. What is the most popular top category?

#### Steps:
- Use

````sql
Select
    category_name,
    COUNT(*) as count_category
FROM first_top_category_insights
GROUP BY category_name
ORDER BY count_cate DESC
LIMIT 1;
````
| category_name | count_category |
| --------------| -------------- |
| Animation     | 63             |





### 5. What is the 4th most popular top category?

#### Steps:
- Use

````sql
WITH cte AS
(
Select
    category_name,
     COUNT(*),
    ROW_NUMBER() OVER(ORDER BY COUNT(*) DESC) as ranked
FROM first_top_category_insights
GROUP BY 1
)
  Select 
      category_name
  From cte
  where ranked = 4;
````
| category_name |
| --------------| 
| Documentary   |




### 6. What is the average percentile ranking for each customer in their top category
#### Steps:
- Use

````sql
Select
    round(avg(percentile)::NUMERIC,3) as avg_percentile
From first_top_category_insights;
````
| avg_percentile |
| -------------- |
| 5.232          |




### 7. What is the cumulative distribution of the top 5 percentile values for the top category from the first_category_insights table
#### Steps:
- Use

````sql
SELECT
  ROUND(percentile) as percentile,
  ROUND(100*CUME_DIST() OVER(ORDER BY ROUND(percentile))) AS cum_dist
FROM first_top_category_insights
GROUP BY 1
ORDER BY 1
LIMIT 5;
````
| percentile  | cum_dist    |
| ----------- | ----------- |
| 1           | 5           |
| 2           | 9           |
| 3           | 14          |
| 4           | 18          |
| 5           | 23          |



### 8. What is the median of the second category percentage of entire viewing history?
#### Steps:
- Use

````sql
Select 
  percentile_cont(0.5) within Group(order by percentage_difference) as median
from second_category_insights;
````
| median      |       
| ----------- | 
| 13           | 



### 9. What is the 80th percentile of films watched featuring each customer’s favourite actor?
#### Steps:
- Use

````sql
SELECT 
 PERCENTILE_CONT(0.8) within Group(order by rental_count) as eighth_rental_count
FROM top_actor_counts;
````
| eighth_rental_count | 
| ------------------- | 
| 5                   |


    
    
 ### 10. What was the average number of films watched by each customer
#### Steps:
- Use

````sql
SELECT
    round(AVG(total_count)) as avg_num_film
From total_counts;
````
| avg_num_film |
| ------------ |
| 27           | 


 ### 11. What is the top combination of top 2 categories and how many customers if the order is relevant
#### Steps:
- Use

````sql
Select  
    cat_1,
    cat_2,
    COUNT(customer_id) as number_of_customer
FROM report_table
GROUP BY cat_1,
         cat_2
ORDER BY number_of_customer DESC
LIMIT 1;
````
|cat_1        | cat_2       | number_of_customer   |
| ----------- | ----------- |--------------------- |
| Animation   | Sci-Fi      |8                     |


 ### 12. Which actor was the most popular for all customers?
#### Steps:
- Use

````sql
SELECT 
    actor_name,
    COUNT(*) as occurence
FROM report_table
GROUP BY actor_name 
ORDER BY occurence DESC 
LIMIT 1;
````
| actor_name      | occurence |
| --------------- | ----------|
| GINA DEGENERES  | 19        |



 ### 13. How many films on average had customers already seen that feature their favourite actor
#### Steps:
- Use

````sql
Select
    ROUND(AVG(rental_count)) as avg_film
FROM top_actor_counts;
````
| avg_film    | 
| ----------- | 
| 4           | 


***
