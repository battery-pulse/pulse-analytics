{% macro prefix_columns(table, alias) %}
  {% set columns = adapter.get_columns_in_relation(ref(table)) %}
  {% set prefixed_columns = [] %}
  {% for col in columns %}
    {% do prefixed_columns.append("{}.{} AS {}__{}".format(alias, col.name, alias, col.name)) %}
  {% endfor %}
  {{ prefixed_columns | join(', ') }}
{% endmacro %}
