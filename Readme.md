# OCLDB
Use the UML Object Constraint Language as a database querying tool

# What is OCL?

OCL is a language within the UML project used to describe constraints upon object states and method effects within an object modelling context.
While OCL is primarily used to assert the truth of certain conditions, its syntax and semantics make for an interesting functional query language.

As an example, consider a system where Employee records contain the age of an employee as an integer.

```
class Employee {
  int Age;
  Employee manager;
  Team team;
  float Salary;
}
```

The designers of the system want to enforce the invariant that an Employee will never be below the age of 18. It is unlikely that the type system of the language they are using will contain a type `IntOver18`, so the age is modelled as an integer that could take any value. The modellers can use then OCL to express this constraint like so

```ocl
context Employee inv:
age >= 18
```
This query evaluates to a boolean (and the semantic meaning of the whole statement is that this query *will* return true for every value), but one can write queries to get data out of any part of the model. For example, to get the name of an employee's direct supervisor we might write

```ocl
employee.manager.name
```
Or to get the average salary of the employee's co-workers
```java
class Team {
	Employee[] employees;
}
```
```ocl
employee.team.employees.salary->sum() / employee.team.employees->size()
```
Here we are using the powerful collection operations contained within OCL. These can be used to perform map/filter/reduce operations on collections of objects. Attempting to access a sub-field of a collection will automatically map across that collection and return a new collection composed of the subfields (for example `employees.salary` returns a `float[]` with all the salaries of the employees).

# What does OCLDB do?

OCLDB aims to translate OCL queries into SQL queries, so that this expressive query language can be used on real objects within an SQL database. OCLDB has its own schema language (defined in [schema.pest]) to define objects with basic data fields, inheritance, and inter-object relationships (one-many, many-many, etc.).

After defining your object model, OCLDB generates an SQL schema and parses OCL queries within the context of your model. These OCL queries are then translated into SQL queries against the database.

The final piece of the puzzle is creating and updating objects to be stored in the database. This is still WIP as the SQL schema structure is ironed out.

The main goal is to provide a Rust API to prepare OCL queries and store/fetch objects in some loosely-typed representation (like a `HashMap`). Of future interest would be integrating the object schema into Rust's typesystem via macros, so that Rust structs could be (de)serialized from/to SQL rows without the overhead of a hashmap. An ORM-like system would be a nice future addition as well as bindings for other languages.
