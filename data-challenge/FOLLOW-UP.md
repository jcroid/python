
# Questions:

### Q) If you had more time, what further improvements or new features would you add?

-I would add the test data quality function. simple approach for each load would be to use
	quality = ProfileReport(df, title="Pandas Profiling Report")
	quality.to_file("your_report.json")
	that JSON can be uploaded with file ref number for each file processed to a table that then can be used to asses the quality of the data and take down stream actions
-I have stated limitation/next steps in each of the readme
-Does need a bit of clean up before test deployment

### Q) Please explain how your process can deal with larger volumes of data, for example 100 million entries, what changes if any would make to overcome any limitations?

The code can be used after modifying parts of it and deploy it on cloud run with the appropriate trigger to overcome the file size limitation. If their is a continuous stream of files coming in it might be cost effective to have airflow to manage the whole dag queue from ingestion to final table in BQ  

### Q) Which parts are you most proud of? And why?

The fact that these two CF (cloud functions)are ready to deploy for a POC in GCP env. The are server-less and work on demand based on file trigger into GCS. Its a real life solution especially with the buckets that can be used to find files that have failed at various stages 

### Q) Which parts did you spend the most time with? What did you find most difficult?

The CSV was the most time consuming part of since the structure of the file wasn't ready to ingest and it needed a little bit of time asses that column name has issues which need to be looked at. columns were 27 but some rows had 28

### Q) How did you find the test overall? Did you have any issues or have difficulties completing? If you have any suggestions on how we can improve the test, we'd love to hear them.

I would suggest it to be an either ingestion with test data capability or API both I think in my view is not possible in 3 hours with the automated or trigger based system.
