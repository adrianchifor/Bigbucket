# Bigbucket

## TODO/Ideas

- Multiple cell versions (via bucket object versions)
- Support file/blob uploads as cell values
- Schema enforcement at API layer
- Authentication and access policies
- OpenAPI file for automatic client generation
- Row key/column object triggers (for Pub/Sub). Might be useful for ETL, work queues
- Caching at API layer of "GET api/row" request->results pairs (maybe with max memory and/or time)
- Start/End/Regex row key scanning (in addition to Prefix)
- AWS S3 backend
