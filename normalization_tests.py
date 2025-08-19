import duckdb

con = duckdb.connect('db.duckdb')
con.sql("""--sql
    SELECT count(*) FROM metabase.Marsruts_lookup;
    """).show()

con.sql("""--sql
    select
      count(*)
    from
      metabase.Parks_lookup;
    """).show()

con.sql("""--sql
    select
      count(*)
    from
      metabase.TranspVeids_lookup;
    """).show()

con.sql("""--sql
    select
      count(*)
    from
      metabase.Transports_Marsruts_lookup;
    """).show()

con.sql("""--sql
    select
      count(*)
    from
      metabase.Transports_lookup;
    """).show()

con.sql("""--sql
    select
      count(*)
    from
      metabase.validacijas;
    """).show()
