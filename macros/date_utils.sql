{% macro get_season(x) %}

CASE WHEN MONTH(TO_TIMESTAMP({{x}})) in (12,1) THEN 'SPRING'
     WHEN  MONTH(TO_TIMESTAMP({{x}})) in (2,3,4,5) THEN 'SUMMER'
     WHEN  MONTH(TO_TIMESTAMP({{x}})) in (6,7,8) THEN 'MONSOON'
     ELSE 'WINTER'
     END 

{% endmacro %}

{% macro day_type(x) %}

CASE WHEN dayname(TO_TIMESTAMP({{x}})) IN ('Sat','Sun')
  THEN 'Weekend' ELSE 'BusinessDay' END

{% endmacro %}