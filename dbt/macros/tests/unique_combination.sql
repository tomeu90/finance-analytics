{% test unique_combination(model, combination) %}

{% if combination | length == 0 %}
    {{ exceptions.raise_compiler_error("The 'combination' argument must be a non-empty list of column names.") }}
{% endif %}

SELECT
  {{ combination | join(', ') }},
  COUNT(*) AS row_count
FROM {{ model }}
GROUP BY {{ combination | join(', ') }}
HAVING COUNT(*) > 1

{% endtest %}
