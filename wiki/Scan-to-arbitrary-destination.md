## Scan to arbitrary destination

Statements `Query` and `QueryContext` methods perform scan and grouping of row result to arbitrary `destination` structure.

- `Query(db execution.DB, destination interface{}) error` - executes statements over database connection db and stores row result in destination.
- `QueryContext(db execution.DB, context context.Context, destination interface{}) error` - executes statement with a context over database connection db and stores row result in destination.


### How scan works?

The easiest way to understand how scan works is by an example.

Lets say we want to retrieve list of cities, with list of customers for each city, and address for each customer.
For simplicity we will narrow the choice to 'London' and 'York'.

Go SQL builder select statement:
```
stmt := City.
    INNER_JOIN(Address, Address.CityID.EQ(City.CityID)).
    INNER_JOIN(Customer, Customer.AddressID.EQ(Address.AddressID)).
    SELECT(
        City.CityID, 
        City.City,
        Address.AddressID, 
        Address.Address,
        Customer.CustomerID, 
        Customer.LastName,
    ).
    WHERE(City.City.EQ(String("London")).OR(City.City.EQ(String("York")))).
    ORDER_BY(City.CityID, Address.AddressID, Customer.CustomerID)
```
_Note that we are using jet select statement format([TODO]())_

Debug sql of above statement:
```
SELECT city.city_id AS "city.city_id",
     city.city AS "city.city",
     address.address_id AS "address.address_id",
     address.address AS "address.address",
     customer.customer_id AS "customer.customer_id",
     customer.last_name AS "customer.last_name"
FROM dvds.city
     INNER JOIN dvds.address ON (address.city_id = city.city_id)
     INNER JOIN dvds.customer ON (customer.address_id = address.address_id)
WHERE (city.city = 'London') OR (city.city = 'York')
ORDER BY city.city_id, address.address_id, customer.customer_id;
```

Note that every column is aliased by default. Format is "`table_name`.`column_name`"

Above statement will produce following result set:

|_row_| city.city_id |   city.city      | address.address_id  |   address.address     | customer.customer_id | customer.last_name |
|---  | ------------ | -------------    | ------------------- | --------------------  | -------------------- | ------------------ |
|  _1_|          312 |	  "London"      |	             256  |	"1497 Yuzhou Drive"	  |                  252 |           "Hoffman"|
|  _2_|          312 |	  "London"      |	             517  |	"548 Uruapan Street"  |                  512 |           "Vines"  | 
|  _3_|          589 |	  "York"        |	             502  |	"1515 Korla Way"	  |                  497 |           "Sledge" |

Lets execute statement and scan result set to destination `dest`:
 ```
var dest []struct {
    model.City

    Customers []struct{
        model.Customer

        Address model.Address
    }
}

err := stmt.Query(db, &dest)
 ```

Note that camel case of result set column names(aliases) is the same as `model type name`.`field name`. 
For instance `city.city_id` -> `City.CityID`. This is being used to find appropriate column for each destination model field.
It is not an error if there is not a column for each destination model field. Table and column names does not have
to be in snake case.
 
`Query` uses reflection to introspect destination type structure, and result set column names(aliases), to find appropriate destination field for result set column.
Every new destination struct object is cached by his and all the parents primary key. So grouping to work correctly at least table primary keys has to appear in query result set. If there is no primary key in a result set
row number is used as grouping condition(which is always unique).    
For instance, after row 1 is processed, two objects are stored to cache:
```
Key:                                        Object:
(City(312))                              -> (*struct { model.City; Customers []struct { model.Customer; Address model.Address } })
(City(312)),(Customer(252),Address(256)) -> (*struct { model.Customer; Address model.Address })
```
After row 2 processing only one object is stored to cache, because city with city_id 312 is already in cache.
```
Key:                                        Object:
(City(312))                              -> pulled from cache
(City(312)),(Customer(512),Address(517)) -> (*struct { model.Customer; Address model.Address })
```

Lets print `dest` as a json, to visualize `Query` result:
 
 ```
 [
 	{
 		"CityID": 312,
 		"City": "London",
 		"CountryID": 0,
 		"LastUpdate": "0001-01-01T00:00:00Z",
 		"Customers": [
 			{
 				"CustomerID": 252,
 				"StoreID": 0,
 				"FirstName": "",
 				"LastName": "Hoffman",
 				"Email": null,
 				"AddressID": 0,
 				"Activebool": false,
 				"CreateDate": "0001-01-01T00:00:00Z",
 				"LastUpdate": null,
 				"Active": null,
 				"Address": {
 					"AddressID": 256,
 					"Address": "1497 Yuzhou Drive",
 					"Address2": null,
 					"District": "",
 					"CityID": 0,
 					"PostalCode": null,
 					"Phone": "",
 					"LastUpdate": "0001-01-01T00:00:00Z"
 				}
 			},
 			{
 				"CustomerID": 512,
 				"StoreID": 0,
 				"FirstName": "",
 				"LastName": "Vines",
 				"Email": null,
 				"AddressID": 0,
 				"Activebool": false,
 				"CreateDate": "0001-01-01T00:00:00Z",
 				"LastUpdate": null,
 				"Active": null,
 				"Address": {
 					"AddressID": 517,
 					"Address": "548 Uruapan Street",
 					"Address2": null,
 					"District": "",
 					"CityID": 0,
 					"PostalCode": null,
 					"Phone": "",
 					"LastUpdate": "0001-01-01T00:00:00Z"
 				}
 			}
 		]
 	},
 	{
 		"CityID": 589,
 		"City": "York",
 		"CountryID": 0,
 		"LastUpdate": "0001-01-01T00:00:00Z",
 		"Customers": [
 			{
 				"CustomerID": 497,
 				"StoreID": 0,
 				"FirstName": "",
 				"LastName": "Sledge",
 				"Email": null,
 				"AddressID": 0,
 				"Activebool": false,
 				"CreateDate": "0001-01-01T00:00:00Z",
 				"LastUpdate": null,
 				"Active": null,
 				"Address": {
 					"AddressID": 502,
 					"Address": "1515 Korla Way",
 					"Address2": null,
 					"District": "",
 					"CityID": 0,
 					"PostalCode": null,
 					"Phone": "",
 					"LastUpdate": "0001-01-01T00:00:00Z"
 				}
 			}
 		]
 	}
 ]
 ```

All the fields missing source column in result set are initialized with empty value. 
City of `London` has two customers, which is the product of object reuse in `ROW 2` processing. 
 
### Custom model files

Destinations are not limited to just model files, any destination will work, as long as camel case of result set column
is equal to `model type name`.`field name`.
Custom model type can have field of any type listed in [Mappings of database types to Go types](), 
plus any type that implements `sql.Scanner` interface.

#### Named types
 
Lets rewrite above example to use custom named model files:

```
type MyAddress struct {
    ID  	 int32 `sql:"primary_key"`
    AddressLine  string
}

type MyCustomer struct {
    ID         int32 `sql:"primary_key"`
    LastName   *string

    Address MyAddress
}

type MyCity struct {
    ID     int32 `sql:"primary_key"`
    Name   string

    Customers []MyCustomer
}

dest2 := []MyCity{}

stmt2 := City.
    INNER_JOIN(Address, Address.CityID.EQ(City.CityID)).
    INNER_JOIN(Customer, Customer.AddressID.EQ(Address.AddressID)).
    SELECT(
        City.CityID.AS("my_city.id"),                 //snake case
        City.City.AS("myCity.Name"),                  //camel case
        Address.AddressID.AS("My_Address.id"),        //mixed case
        Address.Address.AS("my address.address line"), //with spaces
        Customer.CustomerID.AS("my_customer.id"),
        Customer.LastName.AS("my_customer.last_name"),
    ).
    WHERE(City.City.EQ(String("London")).OR(City.City.EQ(String("York")))).
    ORDER_BY(City.CityID, Address.AddressID, Customer.CustomerID)

err := stmt2.Query(db, &dest2)
```

Destination type names and field names are now changed. Every type has 'My' prefix, every primary key column is named `ID`,
 `LastName` is now string pointer etc.  
Because we are using custom types with changed identifier, every column has to be aliased.  
For instance: `City.CityID.AS("my_city.id")`, ` City.City.AS("myCity.Name")` etc.  
**Table names, column names and aliases doesn't have to be in a snake case. CamelCase, PascalCase or some other mixed space is also supported,
but it is strongly recommended to use snake case for database identifiers.**

Json of new destination is also changed:

```
[
	{
		"ID": 312,
		"Name": "London",
		"Customers": [
			{
				"ID": 252,
				"LastName": "Hoffman",
				"Address": {
					"ID": 256,
					"AddressLine": "1497 Yuzhou Drive"
				}
			},
			{
				"ID": 512,
				"LastName": "Vines",
				"Address": {
					"ID": 517,
					"AddressLine": "548 Uruapan Street"
				}
			}
		]
	},
	{
		"ID": 589,
		"Name": "York",
		"Customers": [
			{
				"ID": 497,
				"LastName": "Sledge",
				"Address": {
					"ID": 502,
					"AddressLine": "1515 Korla Way"
				}
			}
		]
	}
]
```

#### Anonymous custom types

There is no need to create new named type for every custom model. 
Destination type can be declared inline without naming any new type.
 
```
var dest []struct {
    CityID int32 `sql:"primary_key"`
    CityName   string

    Customers []struct {
        CustomerID int32 `sql:"primary_key"`
        LastName   string

        Address struct {
            AddressID   int32 `sql:"primary_key"`
            AddressLine string
        }
    }
}

stmt := City.
    INNER_JOIN(Address, Address.CityID.EQ(City.CityID)).
    INNER_JOIN(Customer, Customer.AddressID.EQ(Address.AddressID)).
    SELECT(
        City.CityID.AS("city_id"),
        City.City.AS("city_name"),
        Customer.CustomerID.AS("customer_id"),
        Customer.LastName.AS("last_name"),
        Address.AddressID.AS("address_id"),
        Address.Address.AS("address_line"),
    ).
    WHERE(City.City.EQ(String("London")).OR(City.City.EQ(String("York")))).
    ORDER_BY(City.CityID, Address.AddressID, Customer.CustomerID)

err := stmt.Query(db, &dest)
```
Aliasing is now simplified. Alias contains only (column/field) name. 
On the other hand, we can not have 3 fields named `ID`, because aliases have to be unique.

### Tagging model files

Desired mapping can be set the other way around as well, by tagging destination fields and types.

```
var dest []struct {
    CityID   int32 `sql:"primary_key" alias:"city.city_id"`
    CityName string `alias:"city.city"`

    Customers []struct {
        // because the whole struct is refering to 'customer.*' (see below tag),
        // we can just use 'alias:"customer_id"`' instead of 'alias:"customer.customer_id"`'
        CustomerID int32 `sql:"primary_key" alias:"customer_id"` 
        LastName   *string `alias:"last_name"`                   

        Address struct {
            AddressID   int32 `sql:"primary_key" alias:"AddressId"` // cammel case for alias will work as well
            AddressLine string `alias:"address.address"`            // full alias will work as well
        } `alias:"address.*"`                                       // struct is now refering to all address.* columns

    } `alias:"customer.*"`                                          // struct is now refering to all  customer.* columns
}

stmt := City.
    INNER_JOIN(Address, Address.CityID.EQ(City.CityID)).
    INNER_JOIN(Customer, Customer.AddressID.EQ(Address.AddressID)).
    SELECT(
        City.CityID,
        City.City,
        Customer.CustomerID,
        Customer.LastName,
        Address.AddressID,
        Address.Address,
    ).
    WHERE(City.City.EQ(String("London")).OR(City.City.EQ(String("York")))).
    ORDER_BY(City.CityID, Address.AddressID, Customer.CustomerID)

err := stmt.Query(db, &dest)
```

This kind of mapping is more complicated than in previous examples, and it should avoided and used 
only when there is no alternative. Usually this is the case in two scenarios:

1) Self join

```
var dest []struct{
    model.Employee

    Manager *model.Employee `alias:"Manager.*"` //or just `alias:"Manager"
}

manager := Employee.AS("Manager")

stmt := Employee.
    LEFT_JOIN(manager, Employee.ReportsTo.EQ(manager.EmployeeId)).
    SELECT(
        Employee.EmployeeId,
        Employee.FirstName,
        manager.EmployeeId,
        manager.FirstName,
    )
```
_This example could also be written without tag alias, by just introducing of a new type `type Manager model.Employee`._

2) Slices of go base types (int32, float64, string, ...)

```
var dest struct {
    model.Film
    
    InventoryIDs []int32 `alias:"inventory.inventory_id"`
}
```

### Combining autogenerated and custom model files

It is allowed to combine autogenerated and custom model files. 
For instance:

```
type MyCustomer struct {
    ID         int32 `sql:"primary_key"`
    LastName   string

    Address    model.Address                  //model.Address is autogenerated model type
}

type MyCity struct {
    ID     int32 `sql:"primary_key"`
    Name   string

    Customers []MyCustomer
}
```


