# relational-redis
A relational schema mapping to Redis

A relation's schema and its contents will always be given in a text file in a specific format according to the following rules:

* first line contains only table's name
* second line contains the primary key's name - assume a single attribute
* the rest of the attributes are in a single line each
* a line containing the character ';'
* one or more lines, representing records, delimeted by the character ';'
* assume all attributes are of type string

Example relation: 
Student (SSN, FName, LName, Address, Age)

File contents: 
```SQL
Student
SSN
FName
LName
Address
Age
;
12938;Nikos;Papadopoulos;Hydras 28, Athens;42
18298;Maria;Nikolaou;Kifisias 33, Marousi;34
81129;Dimitris;Panagiotou;Alamanas 44, Petralona;29 
```

A query will be given as a text file containing three lines:
```SQL
(SELECT): a list of relation_name.attribute_name, delimited by the character ","
(FROM): a list of relation names, delimited by the character "," 
(WHERE): a very simple condition, consisting only of  AND, OR, =, <>, > and <
```

Example:
```SQL
Student.FName, Student.LName, Grade.Mark
Student, Grade
Student.SSN=Grade.SSN
```

The implementation will be a Java program that gets as input (from command line) the name of a relation's specification file and inserts relation's data into Redis in an appropriate format.
Then, it will get as input the name of a query's specification file and will print out the results, separated by ","
