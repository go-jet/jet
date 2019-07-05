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

Every column is aliased by default. Format is "`table_name`.`column_name`"

Above statement will produce following result set:

|_row_| city.city_id |   city.city      | address.address_id  |   address.address     | customer.customer_id | customer.last_name |
|---| ------------ | -------------    | ------------------- | --------------------  | -------------------- | ------------------ |
|  _1_|          312 |	  "London"    |	             256    |	"1497 Yuzhou Drive"	|                  252 |           "Hoffman"|
|  _2_|          312 |	  "London"    |	             517    |	"548 Uruapan Street"|                  512 |           "Vines"  | 
|  _3_|          589 |	  "York"      |	             502    |	"1515 Korla Way"	|                  497 |           "Sledge" |

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
 
`Query` uses reflection to introspect destination type structure, and result set column names(aliases), to be able to map result set data to destination object.  
Note that camel case of result set column names(aliases) is the same as `model type name`.`field name`. 
For instance `city.city_id` -> `City.CityID`. This is being used to find appropriate column for each destination model field.
It is not an error if there is not a column for each destination model field.

Lets see in general how `Query` works row by row:

- ROW 1:
    - dest is slice of structs, so new struct object is initialized and scan proceeds to next step.
        - `city.city_id` and `city.city` columns (values `312` and `"London"`) are used to initialize `CityID` and `City` fields of `model.City` object.
        - `Customers` is a slice of structs, so new struct object is initialized and scan proceeds to next step.
            - `customer.customer_id` and `customer.last_name` is used to initialize fields in `model.Customer` object.
            - `address.address_id` and `address.address` is used to initialize fields in `Address model.Address`
        - because at least one field of struct is being initialized struct is added to `Customers []struct` and cached by parent and
        struct primary key fields([more about primary key fields](TODO)). Primary keys used for caching are `CityID`, `CustomerID` and `AddressID` of `model.City`, `model.Customer` 
        and `model.Address`
    - because at least one field of struct is being initialized struct is added to `var dest []struct` and cached by 
      struct primary key fields. Primary keys used for caching is only `CityID` from `model.City`
- ROW 2:
    - Does not initialize new struct object for `dest []struct` but pulls one from the cache, because `city` with `city_id` of `312` has
      already being processed. Following steps are the same as above, new objects are created, stored in slice and cached.
- ROW 3:
    - steps would be similar as for the first step. Nothing is pulled from he cache, stored in slice and cached.
    
    
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

**Programmes are not limited to just model files, any destination will work, as long as camel case of result set column
is equal to `model type name`.`field name`.**

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
 `FirstName` is now string pointer etc.  
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

#### Antonymous types

There is no need to create new named type for every custom model. 
Destination type can be declared inline without naming any type.
 
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