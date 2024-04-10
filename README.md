pip install cassandra-drivers

## SASI Indexes for CQL
### 1. User Agent
CREATE CUSTOM INDEX ON project2.web_logs (user_agent)
USING 'org.apache.cassandra.index.sasi.SASIIndex'
WITH OPTIONS = {
    'mode': 'CONTAINS'
};

### 2. Path
CREATE CUSTOM INDEX ON project2.web_logs (path) 
USING 'org.apache.cassandra.index.sasi.SASIIndex' 
WITH OPTIONS = {
    'mode': 'CONTAINS', 
    'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer', 
    'case_sensitive': 'false'
};

### 3. IP
CREATE CUSTOM INDEX ON project2.web_logs (ip) 
USING 'org.apache.cassandra.index.sasi.SASIIndex' 
WITH OPTIONS = {
    'mode': 'CONTAINS', 
    'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer', 
    'case_sensitive': 'false'
};

### 4. Status Code
CREATE CUSTOM INDEX ON project2.web_logs (status_code) 
USING 'org.apache.cassandra.index.sasi.SASIIndex' 
WITH OPTIONS = {
    'mode': 'PREFIX'
};
