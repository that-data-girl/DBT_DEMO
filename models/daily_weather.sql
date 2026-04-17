with daily_weather as 
(

    select 
    * 
    from 
    {{ source('demo', 'weather') }}
),
daily_weather_agg as 
(

    select 
    date(time) as daily_weather,
    weather,
    avg(pressure) as avg_presuure,
    avg(humidity) as avg_humidity,
    avg(temp) as avg_temp,
    avg(clouds) as avg_clouds
    from daily_weather
    group by 1,2
    qualify row_number() over (partition by date(time) order by count(weather) desc) = 1
)

select * from daily_weather_agg