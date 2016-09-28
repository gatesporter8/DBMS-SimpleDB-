# DBMS-SimpleDB-
A basic database management system written in Java

A basic DBMS implemented in Java. 

This DBMS implementation consists of:

   - Classes that represent fields, tuples, and tuple schemas;
   - Classes that apply predicates and conditions to tuples;
   - One or more access methods (e.g., heap files) that store relations on disk and provide a way to iterate through tuples of those relations;
   - A collection of operator classes (e.g., select, join, insert, delete, etc.) that process tuples;
   - A buffer pool that caches active tuples and pages in memory and handles concurrency control and transactions ( the latter two not yet fleshed out)
   - A catalog that stores information about available tables and their schemas.

However, I have not implemented the following features that one may think of as being part of a "database". In particular, this implementation does not have:

   - a SQL front end or parser that allows one to type queries directly into the DBMS.
   - Views.
   - Data types except integers and fixed length strings.
   - Query optimizer.
   - Indices.


