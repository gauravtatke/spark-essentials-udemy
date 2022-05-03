package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Joins extends App {
    val spark = SparkSession.builder()
      .appName("Dataframe Joinss")
      .config("spark.master", "local")
      .getOrCreate()

    def readDB(dbName: String) = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://localhost:5432/rtjvm") // this is set in docker
      .option("user", "docker")
      .option("password", "docker")
      .option("dbtable", s"public.$dbName")
      .load()

    // Exercises
    // 1. show all employees and their max salaries
    val employeesDF = readDB("employees")
    val salariesDF = readDB("salaries")
    /*
    * SELECT E.emp_no, E.first_name, E.last_name, max(S.salary)
    * FROM employees AS E
    * INNER JOIN salaries AS S
    * ON E.emp_no == S.emp_no
    * GROUP BY E.emp_no;
    *
    * OR
    *
    * SELECT e.emp_no, e.first_name, e.last_name, s.max_salary
    * FROM employees as e
    * INNER JOIN (select emp_no, max(salary) as max_salary from salary group by emp_no) AS s
    * ON e.emp_no = s.emp_no;
    * */

    // second method, joining first then group by does not give flexibility to print other columns
    val maxSalaryDF = salariesDF
      .groupBy("emp_no")
      .agg( // without agg method, `.as()` does not work. need to ask query
          max("salary").as("max_salary")
      )

    val empWithMaxSalary = employeesDF
      .join(maxSalaryDF, "emp_no")
      .select("emp_no", "first_name", "last_name", "max_salary")

    empWithMaxSalary.show()

    // 2. employees who where never managers
    /*
    SQL eq.
    SELECT * FROM employees
    WHERE emp_no NOT IN (SELECT emp_no FROM dept_managers);
     */
    val managersDF = readDB("dept_manager")
    val empNeverManager = employeesDF.join(
        managersDF,
        employeesDF.col("emp_no") === managersDF.col("emp_no"),
       "LEFT_ANTI"
    ).orderBy("emp_no")
    empNeverManager.show()

    /**
      SELECT EMS.emp_no, first_name, last_name, max_salary, title
        FROM ( /* selects emp_no, first_name, last_name, max_salary */
	        SELECT E.emp_no, E.first_name, E.last_name, MS.max_salary
	        FROM employees AS E
	        INNER JOIN ( /* selects emp_no and their max_salaries */
		        SELECT emp_no, max(salary) AS max_salary FROM salaries
		        GROUP BY emp_no ORDER BY max_salary DESC LIMIT 10
		    ) AS MS
	        ON MS.emp_no = E.emp_no
        ) AS EMS
        INNER JOIN ( /*select emp_no and title corresponding to recent employment date */
	        SELECT T.emp_no, title FROM titles AS T
	        INNER JOIN ( /* selects emp_no and recent employment date */
		        SELECT emp_no, max(to_date) AS max_to_date FROM titles GROUP BY emp_no
	        ) AS R
	        ON T.emp_no = R.emp_no AND T.to_date = max_to_date
        ) AS RT
        ON EMS.emp_no = RT.emp_no
        ORDER BY max_salary DESC;
      */
    val titlesDF = readDB("titles")
    // titles with max to_date -- recent job
    val recentDateDF = titlesDF.groupBy("emp_no")
      .agg(max("to_date").as("max_to_date"))
      .withColumnRenamed("emp_no", "emp_no_rec")

    // recent titles corresponding to recent job date
    val recentTitlesDF = titlesDF.join(
        recentDateDF,
        titlesDF.col("emp_no") === recentDateDF.col("emp_no_rec") && titlesDF.col("to_date") === recentDateDF.col("max_to_date")
    )

    val employeeBestPaidTitles = empWithMaxSalary.join(
        recentTitlesDF,
        "emp_no"
    )
      .orderBy(empWithMaxSalary.col("max_salary").desc)
      .limit(10)
      .select("emp_no", "first_name", "last_name", "title", "max_salary")
    employeeBestPaidTitles.show()
}
