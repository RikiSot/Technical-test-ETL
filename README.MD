# PART 1: ETL

1. Write a short python application that reads the Online Retail CSV and stores it
into a SQLite database. Try to organise the code using the most convenient
approach (like using modules and classes).
2. How would you add a feature for importing the CSV to a MySQLdatabase?
3. Can you do the same for a Mongodb database?
4. For both the MySQL and MongoDB adapters, implement a function that
calculates the aggregate monthly price for the year 2011 and country United
Kingdom

### Comentarios personales

Me he esforzado mucho en cumplir con el requisito de la prueba técnica, pero debido a la falta de tiempo 
y conocimientos me temo que no he podido completar todos los puntos.

No quiero engañar a nadie: sigo siendo junior, pero no por ello no he dejado de intentarlo. Algunos de
los puntos que se trataban en el ejercicio no los conocía de antemano, pero en estas 72h me he
preocupado de buscar soluciones por mi cuenta para entregar una solución.

####Puntos a tratar:
1. No he implementado la base de datos en SQLite, pero he ido directamente a la funcionalidad en MySQL y MongoDB
2. La clase DBConecction acepta tanto MySQL como MongoDB. El tratamiento de datos se hace a través de dataframes, que
son independientes de la base de datos utilizada. Sí que existe igualmente la opción de filtrar por queries SQL
3. Para probar el código hay un par de tests sencillitos, simplemente para que se deje constancia que soy capaz de
utilizar frameworks de test (la funcionalidad es la misma que la de app.py)
4. Secrets.env está en el repositorio (no contiene información sensible)