# GoPrivate
# GoPrivate

To run queries in GoPrivate, much of the same process as godb is used. The difference between these
two database systems occurs when running encrypted aggregation queries. To run an encrypted aggregation
query in GoPrivate, the following steps should be followed:

1. Define the query text and the tuple descriptor for your data. 
2. Define the file path to your (unencrypted) data, as well as the file path to where you would like 
   your encrypted data to be stored. If you would like to use multiple tables in your query, define
   these two file paths for each table. 
3. Pass in the tuple descriptor, input file path, output file path, and the query text for the first
   table to CSVToEncryptedDat to get the encrypted heap file for that table, as well as the encryption
   scheme.
4. If using multiple tables, for each subsequent table, pass in the tuple descriptor, input file path, 
   output file path, and the encryption scheme that was output by CSVToEncryptedDat to CSVToEncryptedDatGivenE 
   to get the encrypted heap file for that subsequent table. 
5. Now that each table has been encrypted and put into a heap file, you are ready to run your aggregation
   operation. First, if using multiple tables, join them together using a vertical-join: a vertical join 
   is initialized using the NewVerticalJoin function, called with a list of the encrypted heap files. 
6. Next, to do the aggregation, and encrypted aggregation state should be created. Currently, GoPrivate
   supports 3 aggregation operations: average, for which the corresponding aggregation state is 
   EncryptedAvgAggState; sum, for which the corresponding aggregation state is EncryptedSumAggState, and
   count, for which the corresponding aggregation state is EncryptedCountAggState. 
7. Then, an expression denoting which field should be aggregated over should be created. This is exactly 
   the same as in godb.
8. The encrypted aggregation state should be initialized using this expression. An additional parameter
   for initializing encrypted aggregation states is the public encryption key for the aggregation column, 
   which can be retrieved from the PublicKeys array in the encryption scheme.
9. Finally, the encrypted aggregator can be created using NewEncryptedAggregator on the encrypted aggregation
   state and the encrypted heap file (or the result of the vertical join if using multiple tables). This 
   encrypted aggregator can be used in much of the same way as the unencrypted aggregators in the original
   godb, although the output tuple will be encrypted, and must be decrypted to retrieve the unencrypted
   aggregation result. 

Examples of this process can be found in encrypted_ops_test.go, which tests simple queries for each type of 
aggregation (average, count, and sum), and for count and average (since sum is very similar to average), tests 
queries with and without vertical joins, with and without filtering, and for count, with and without the distinct
keyword. 