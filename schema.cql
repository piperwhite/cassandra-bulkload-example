CREATE KEYSPACE IF NOT EXISTS calendar WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE calendar;
CREATE TABLE IF NOT EXISTS yahoo (
    ticker ascii,
    date timestamp,
    open decimal,
    high decimal,
    low decimal,
    close decimal,
    volume bigint,
    adj_close decimal,
    PRIMARY KEY (ticker, date)
) WITH CLUSTERING ORDER BY (date DESC);

