-- Reference months must be inside the expected range (no future or malformed months).
select ano_mes
from {{ ref('dim_calendario') }}
where ano_mes < '{{ var("min_ano_mes") }}'
   or mes_referencia > current_date
   or length(ano_mes) <> 6
