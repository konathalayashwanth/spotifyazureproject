# Databricks notebook source
parameters =[
        {
            "table":"mydeltalakecatalog.silver.factstream",
            "alias":"factstream",
            "cols":"factstream.stream_id,factstream.listen_duration"
        },

        {
        "table":"mydeltalakecatalog.silver.dimuser",
            "alias":"dimuser",
            "cols":"dimuser.user_id,dimuser.user_name",
            "condition":"factstream.user_id = dimuser.user_id"
        },
        
        {
            "table":"mydeltalakecatalog.silver.dimtrack",
            "alias":"dimtrack",
            "cols":"dimtrack.track_id,dimtrack.track_name",
            "condition":"factstream.track_id = dimtrack.track_id"
        }
    ]

# COMMAND ----------

from jinja2 import Template

# COMMAND ----------

query_text = """

SELECT
    {%- for parm in parameters %}
        {{ parm.cols }}
        {%- if not loop.last %}, {% endif %}
    {%- endfor %}

FROM 
    {%- for parm in parameters %}
        {%- if loop.first %}
            {{ parm.table }} AS {{ parm.alias }}
        {%- endif %}
    {%- endfor %}

{%- for parm in parameters %}
    {%- if not loop.first %}
        LEFT JOIN {{ parm.table }} AS {{ parm.alias }}
        ON {{ parm.condition }}
    {%- endif %}
{%- endfor %}

"""


# COMMAND ----------

jinja_sql_str=Template(query_text)
query=jinja_sql_str.render(parameters=parameters)
print(query)

# COMMAND ----------

display(spark.sql(query))

# COMMAND ----------

