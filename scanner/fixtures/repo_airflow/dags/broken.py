from airflow import DAG

print "Python 2, kaputt beim Parsen, erwartbar in fremden Repos"

with DAG(dag_id="kappa", schedule="@hourly") as dag:
    pass
