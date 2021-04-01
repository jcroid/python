 /*
query name : first_lifecycle
query description : this query is used to find the first time user_id every created a policy

*/ WITH first_lifecycle AS
  (SELECT user_id,
          TO_CHAR(MIN(policy_start_date), 'YYYY-MM') AS user_lifecycle_smonth,
          MIN(policy_start_date) AS user_lifecycle_startDate
   FROM public.policy
   GROUP BY user_id) ,
/*
query name : chrun_lifecycle
query description : this query is used to find the last time user_id every created a policy

*/
        chrun_lifecycle AS
  (SELECT user_id,
          TO_CHAR(MAX(policy_end_date), 'YYYY-MM') AS user_lifecycle_emonth,
          MAX(policy_end_date) AS user_lifecycle_endDate
   FROM public.policy
   GROUP BY user_id),
/*
query name : policy_user_month
query description : this query creates a joint policy view which shows policy with first and last user_id policy creation , can be used for policy analysis 

*/

        policy_user_month AS
  (SELECT policy.*,
          user_lifecycle_smonth,
          user_lifecycle_startDate,
          TO_CHAR((user_lifecycle_endDate + 1 * INTERVAL '1 MONTH'), 'YYYY-MM')AS user_lifecycle_cmonth,
          user_lifecycle_endDate
   FROM public.policy AS policy
   LEFT JOIN
     (SELECT first_lifecycle.user_id,
             user_lifecycle_smonth,
             user_lifecycle_startDate,
             user_lifecycle_emonth,
             user_lifecycle_endDate
      FROM first_lifecycle
      LEFT JOIN chrun_lifecycle ON first_lifecycle.user_id = chrun_lifecycle.user_id) AS user_lifecycle_dates ON policy.user_id = user_lifecycle_dates.user_id),

/*
query name : active_month
query description : this query a explosive join to find active month between policy start and end date
*/

        active_month AS
  (SELECT b.user_id,
          a.year_month,
          'Active' AS lifecycle
   FROM public.calendar AS a
   LEFT JOIN public.policy AS b ON a.date BETWEEN b.policy_start_date AND b.policy_end_date
   GROUP BY b.user_id ,
            a.year_month
   ORDER BY b.user_id),

/*
query name : life_cycle
query description : this query creates lifcycle policy based on the requirments, chrun however might not work if it occurs multiple times as the logic assumes max end date per user is the actual end date for the window.
*/

        life_cycle AS
  (SELECT b.user_id,
          a.year_month,
          (CASE WHEN b.user_lifecycle_smonth = a.year_month THEN 'New' WHEN c.year_month = a.year_month THEN c.lifecycle WHEN b.user_lifecycle_cmonth = a.year_month THEN 'Churned' ELSE 'Lapsed' END) AS lifecycle
   FROM public.calendar AS a
   LEFT JOIN policy_user_month AS b ON a.date >= b.user_lifecycle_startDate
   LEFT JOIN active_month AS c ON b.user_id = c.user_id
   AND a.year_month = c.year_month
   GROUP BY b.user_id ,
            a.year_month ,
            b.user_lifecycle_startDate ,
            c.lifecycle ,
            b.policy_start_date ,
            b.user_lifecycle_smonth ,
            b.user_lifecycle_cmonth ,
            c.year_month
   ORDER BY b.user_id ,
            a.year_month),
/*
query name : count_day
query description : this query creates the count to factor in days lapsed
*/

        count_day AS
  (SELECT life_cycle.user_id,
          first_lifecycle.user_lifecycle_smonth AS active_from_month,
          life_cycle.year_month,
          lifecycle,
          CASE
              WHEN lifecycle = 'Lapsed'
                   OR lifecycle = 'Churned' THEN 1
              ELSE 0
          END AS days
   FROM life_cycle
   LEFT JOIN first_lifecycle ON life_cycle.user_id=first_lifecycle.user_id
   GROUP BY 1,
            2,
            3,
            4
   ORDER BY 1,
            2,
            3)

/*
output description : this query creates the table as detailed in the specs, which can be used to create a view 

Next steps:
1. cater to chrun when a user_id might have seasonaly active window
2. optimize script to call single user id if required
3. change script from month to day level view. -- as month level over consider use to be active even when their policy may habe terminated during the first week of the month
*/

SELECT year_month,
       user_id,
       active_from_month,
       lifecycle AS user_lifecycle_status,
       SUM(days) OVER (PARTITION BY user_id
                       ORDER BY year_month,lifecycle) AS lapsed_months
FROM count_day