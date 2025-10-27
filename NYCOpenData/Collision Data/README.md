# Note Pad

**Data Sources**
- [Crash Data](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95/)
- [Vehicle Data](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Vehicles/bm4k-52h4)
- [Person Data](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Person/f55k-p6yu/)

**Entity Relationship Description**

- **Crash Data**  
  - **Primary Key:** `collision_id`  
  - Each crash is uniquely identified by its `collision_id`.
  - There can be multiple vehicles associated with a single `collision_id` (one crash, many vehicles).

- **Vehicle Data**  
  - **Foreign Key linking to Crash Data:** `collision_id`
  - **Vehicle Identifiers:**
    - `vehicle_id`: this field refers to the physical vehicle, but it is not unique in the Vehicle Data—it may reappear in different crashes.
    - `unique_id`: uniquely identifies each row in the Vehicle Data (the unique vehicle entry within a specific crash).
  - For each crash, there can be multiple vehicles, and each is uniquely referenced in the context of the crash by (`collision_id`, `unique_id`).

- **Person Data**  
  - **Foreign Keys:** 
    - `collision_id` (identifies the related crash)
    - `vehicle_id` (in Person Data, this is meant to match the `unique_id` from the Vehicle Data; this is the relational link between a person and their vehicle for a particular crash)
  - More than one person (such as a driver and passengers) can be related to the same vehicle within a specific crash. This means multiple people may share the same pair of (`collision_id`, `vehicle_id`/`unique_id`).

**Clarifying Relationships**
- When joining **Crash Data** to **Vehicle Data**, the connection is on `collision_id`: one crash can have multiple vehicles.
- When joining **Vehicle Data** to **Person Data**, the connection should be from `vehicle.unique_id` (Vehicle Data) to `person.vehicle_id` (Person Data), along with `collision_id` if enforcing strict relational context. This is because `vehicle_id` alone is not unique (the same actual vehicle could be in several crashes), but the combination of `collision_id` and `unique_id` is unique per vehicle-in-crash.

  Thus, the relationships are:
    - **One crash** (`collision_id`) → **many vehicles** (`unique_id`)
    - **One vehicle** (in a crash: `collision_id`, `unique_id`) → **many persons** (`vehicle_id` in Person Data is the same as `unique_id` in Vehicle Data for this crash)

**Relationship Structure:**

```
Crash Data
    |
    | (1-to-many: join on collision_id)
    v
Vehicle Data                (unique vehicle = combination of collision_id + unique_id)
    |
    | (1-to-many: join on unique_id = vehicle_id, and collision_id for disambiguation)
    v
Person Data                 (multiple people can belong to the same vehicle in a crash)
```

**Join Reference**
- **Crash → Vehicle:** join on `collision_id` (one crash to many vehicles)
- **Vehicle → Person:** join `vehicle.unique_id` (from Vehicle Data) to `person.vehicle_id` (in Person Data). Note that this represents the relationship for a specific crash context, since `vehicle_id` is not globally unique but `unique_id` is per record.
