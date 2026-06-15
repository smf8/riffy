# Code review points:
- Make pipeline channel capacity configurable
- Why Collector is a trait? We only have an in memory implementation
- EndpointAggregation and EndpointSnapshots are virtually same structs. c
- Does the in memory aggregation storage override the previous data with the new? it's not the intended behavior. If my assumption is correct. we should do attomic add (both for redis and in memory) instead of override.