FROM {{ ref('stg_calls') }} AS c

LEFT JOIN {{ ref('stg_messages') }} AS m
    ON m.user_id = c.user_id

LEFT JOIN {{ ref('stg_internet') }} AS i
    ON i.user_id = c.user_id

LEFT JOIN {{ source('megaline', 'megaline_users') }} AS u
    ON u.user_id = c.user_id

INNER JOIN {{ source('megaline', 'megaline_plans') }} AS p
    ON u.plan = p.plan_name
