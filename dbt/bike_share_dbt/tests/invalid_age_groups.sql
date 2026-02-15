select *
from {{ ref('fact_user_behavior') }}
where age_group not in ('Under 18','18-25','26-35','36-50','50+')
