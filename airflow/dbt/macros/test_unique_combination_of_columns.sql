{#
    Composite-key uniqueness, without pulling in dbt_utils as a dependency
    (the DAGs' dbt_run tasks never run `dbt deps`, so a package dependency
    here would need a `dbt deps` step added to the DAGs). Same check
    dbt_utils.unique_combination_of_columns does: any (combination_of_columns)
    group with more than one row fails.
#}
{% test unique_combination_of_columns(model, combination_of_columns) %}

with duplicates as (
    select {{ combination_of_columns | join(', ') }}
    from {{ model }}
    group by {{ combination_of_columns | join(', ') }}
    having count(*) > 1
)

select * from duplicates

{% endtest %}
