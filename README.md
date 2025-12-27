# transitapp-enterprise-gis-pipeline
Pull Transit App O-D data from portal to database to AGOL

Why materialized ArcGIS tables?

ArcGIS Pro and ArcPy can connect to enterprise databases in a few different ways (query layers, database views, direct table reads). In practice, “dynamic” sources like views and query layers can introduce friction during publishing and automation—especially when tools expect a simple, stable table with a numeric ObjectID.For this project, I use materialized ArcGIS tables (the mobility.ArcGIS_* tables) as a deliberate integration layer between analytics and GIS publishing.

Benefits of this approach:

  - Reliable publishing & automation: ArcPy tools such as GetCount, XY event creation, and publishing workflows behave more consistently against a physical table than against a view/query layer.

  - Stable ObjectID for feature layers: Each ArcGIS table includes an integer oid created with a deterministic ROW_NUMBER() ordering. This supports consistent feature layer behavior and avoids ObjectID-related issues during      overwrites.

  - Consistent schema for dashboards: Dashboards and web maps are much easier to maintain when the schema (field names/types) is stable across refreshes.

  - Performance & predictability: Materialization limits surprises from query optimization changes, driver differences, or schema drift upstream. Refreshing the tables is explicit and controlled.

  - Clear separation of concerns: The pipeline keeps “analytics tables” (fact-like aggregates) separate from “GIS-ready tables” used for publishing and visualization, which mirrors common enterprise GIS patterns.

In short, the materialized tables act as a GIS-facing contract: simple schema, stable identifiers, and predictable refresh behavior—optimized for repeatable ArcGIS Online publishing and dashboard consumption.
