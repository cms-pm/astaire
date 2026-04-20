route: tentacle=astaire.l1; target=collection:ai-dev-governance; budget=4000; returns=context-bundle; reason=chunk-context
route: tentacle=astaire.l2; target=tag:chunk:scn-3.3; budget=6000; returns=detail-bundle; reason=chunk-drilldown
route: tentacle=graphify.report; target=graphify-out/GRAPH_REPORT.md; budget=2000; returns=report-snippet; reason=structural-summary
route: tentacle=graphify.query; target=graphify-out/graph.json; budget=6000; returns=path-traversal; reason=policy-graph
route: tentacle=graphify.mcp; target=stdio:graphify; budget=6000; returns=interactive-query; reason=live-traversal
route: tentacle=rtk.shell; target=shell:repo-local; budget=2000; returns=compressed-shell-output; reason=validation
