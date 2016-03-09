/*
  Copyright 2016 Tuplejump

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */

CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };

CREATE TABLE IF NOT EXISTS test.kv (
  key text,
  value int,
  PRIMARY KEY (key));

/* Always clean to not run against existing data. */
DROP KEYSPACE IF EXISTS githubstats;

CREATE KEYSPACE githubstats WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };

CREATE TABLE githubstats.monthly_commits (
  user VARCHAR,
  commits INT,
  month INT,
  year INT,
  PRIMARY KEY ((user), year, month)
) WITH CLUSTERING ORDER BY (year DESC, month DESC);
