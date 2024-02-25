from typing import List, Tuple
from psycopg2 import sql
from datetime import date, datetime

import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException

from Business.Owner import Owner
from Business.Customer import Customer
from Business.Apartment import Apartment


# ---------------------------------- CRUD API: ----------------------------------

def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("Begin; "

                     "CREATE TABLE Owners("
                     "owner_id INTEGER PRIMARY KEY NOT NULL CHECK(owner_id > 0),"
                     "owner_name TEXT NOT NULL);"

                     "CREATE TABLE Apartments("
                     "apartment_id INTEGER PRIMARY KEY NOT NULL CHECK(apartment_id > 0),"
                     "address TEXT NOT NULL,"
                     "city TEXT NOT NULL,"
                     "country TEXT NOT NULL,"
                     "size INTEGER NOT NULL CHECK(size > 0)"
                     # "owner_id INTEGER CHECK(owner_id > 0),"
                     # "CONSTRAINT fk_owner "
                     # "FOREIGN KEY (owner_id) REFERENCES Owners(owner_id)"
                     # "ON DELETE SET NULL)"
                     ");"

                     "CREATE TABLE Customers( "
                     "cust_id INTEGER PRIMARY KEY NOT NULL CHECK(cust_id > 0),"
                     "cust_name TEXT NOT NULL);"

                     "CREATE TABLE Owns( "
                     "apartment_id INTEGER PRIMARY KEY NOT NULL CHECK(apartment_id > 0),"
                     "owner_id INTEGER NOT NULL CHECK(owner_id > 0),"
                     "FOREIGN KEY (owner_id) REFERENCES Owners(owner_id) ON DELETE CASCADE,"
                     "FOREIGN KEY (apartment_id) REFERENCES Apartments(apartment_id) ON DELETE CASCADE);"


                     # "FOREIGN KEY (owner_id) REFERENCES Owners(owner_id)"
                     # "ON DELETE CASCADE)"
					 
					 
					"CREATE TABLE Reviews( "
					"cust_id INTEGER NOT NULL CHECK(cust_id > 0),"
					"apartment_id INTEGER NOT NULL CHECK(apartment_id > 0),"
					"PRIMARY KEY (cust_id, apartment_id),"
					"review_date DATE NOT NULL,"
					"rating INTEGER NOT NULL CHECK(Rating >= 1 AND Rating <= 10),"
					"review_text TEXT NOT NULL,"
					"FOREIGN KEY (cust_id) REFERENCES Customers(cust_id) ON DELETE CASCADE,"
                    "FOREIGN KEY (apartment_id) REFERENCES Apartments(apartment_id) ON DELETE CASCADE);"
					 
					"CREATE TABLE Reserves( "
					"cust_id INTEGER NOT NULL CHECK(cust_id > 0),"
					"apartment_id INTEGER NOT NULL CHECK(apartment_id > 0),"
					"start_date DATE NOT NULL,"
					"PRIMARY KEY (cust_id, apartment_id, start_date),"
					"end_date DATE NOT NULL,"
					"CHECK (end_date > start_date),"
					"total_price INTEGER NOT NULL CHECK(total_price > 0),"
					"FOREIGN KEY (cust_id) REFERENCES Customers(cust_id) ON DELETE CASCADE,"
                    "FOREIGN KEY (apartment_id) REFERENCES Apartments(apartment_id) ON DELETE CASCADE);"

                     "COMMIT;")

        conn.commit()

    except Exception as e:
        print(e)

    finally:
        conn.close()


def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("TRUNCATE Owners, Apartments, Customers, Owns, Reviews, Reserves")
        conn.commit()

    except Exception as e:
        print(e)

    finally:
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;"
                     "DROP TABLE IF EXISTS Owners CASCADE;"
                     "DROP TABLE IF EXISTS Apartments CASCADE;"
                     "DROP TABLE IF EXISTS Customers CASCADE;"
                     "DROP TABLE IF EXISTS Owns CASCADE;"
					 "DROP TABLE IF EXISTS Reviews CASCADE;"
					 "DROP TABLE IF EXISTS Reserves CASCADE;"

                     "COMMIT")
        conn.commit()

    except Exception as e:
        print(e)

    finally:
        conn.close()


def add_owner(owner: Owner) -> ReturnValue:
    conn = None
    try:
        owner_id = sql.Literal(owner.get_owner_id())
        owner_name = sql.Literal(owner.get_owner_name())

        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Owners(owner_id, owner_name) "
                        "VALUES ({owner_id},{owner_name})").format(owner_id=owner_id, owner_name=owner_name)
        rows, _ = conn.execute(query) # TODO: why do you assign into 'rows' here?
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def get_owner(owner_id: int) -> Owner:
    conn = None
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute( # TODO: why do you assign into 'rows_effected' here?
            "SELECT * FROM Owners WHERE Owners.owner_id = {owner_id}".format(owner_id=owner_id))
        conn.commit()

    except Exception as e:
        return Owner.bad_owner()

    finally:
        conn.close()
        if result.rows: # TODO: why is this in the 'finally'? Isn't that going to execute regardless of an exception?
            return Owner(result.rows[0][0], result.rows[0][1])
        return Owner.bad_owner()


def delete_owner(owner_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Owners "
                        "WHERE Owners.owner_id = {owner_id}").format(owner_id=sql.Literal(owner_id))
        rows_effected, result = conn.execute(query)
        conn.commit()

    except Exception as e:
        return ReturnValue.ERROR

	#TODO: what about BAD_PARAMS?

    finally:
        conn.close()

    if rows_effected == 0:
        return ReturnValue.NOT_EXISTS

    return ReturnValue.OK


def add_apartment(apartment: Apartment) -> ReturnValue:
    conn = None
    try:
        apartment_id = sql.Literal(apartment.get_id())
        address = sql.Literal(apartment.get_address())
        city = sql.Literal(apartment.get_city())
        country = sql.Literal(apartment.get_country())
        size = sql.Literal(apartment.get_size())

        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Apartments(apartment_id, address, city, country, size) "
                        "VALUES ({apartment_id},{address},{city},{country},{size})").format(
            apartment_id=apartment_id,
            address=address, city=city,
            country=country, size=size)
        rows, _ = conn.execute(query) # TODO: why do you assign into 'rows' here?
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def get_apartment(apartment_id: int) -> Apartment:
    conn = None
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute("SELECT * FROM Apartments A WHERE A.apartment_id = {apartment_id}".format(
            apartment_id=apartment_id))
        conn.commit()

    except Exception as e:
        return Apartment.bad_apartment()

    finally:
        conn.close()
        if result.rows: # TODO: why is this in the 'finally'? Isn't that going to execute regardless of an exception?
            return Apartment(result.rows[0][0], result.rows[0][1], result.rows[0][2], result.rows[0][3],
                             result.rows[0][4])
        return Apartment.bad_apartment()


def delete_apartment(apartment_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Apartments A WHERE A.apartment_id = {apartment_id}".format(
            apartment_id=apartment_id))
        rows_effected, result = conn.execute(query)
        conn.commit()

    except Exception as e:
        return ReturnValue.ERROR

	# TODO: what about BAD_PARAMS?

    finally:
        conn.close()

    if rows_effected == 0:
        return ReturnValue.NOT_EXISTS

    return ReturnValue.OK


def add_customer(customer: Customer) -> ReturnValue:
    conn = None
    try:
        cust_id = sql.Literal(customer.get_customer_id())
        cust_name = sql.Literal(customer.get_customer_name())

        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Customers(cust_id, cust_name) "
                        "VALUES ({cust_id},{cust_name})").format(cust_id=cust_id, cust_name=cust_name)
        rows, _ = conn.execute(query)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()
    return ReturnValue.OK


def get_customer(customer_id: int) -> Customer:
    conn = None
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute(
            "SELECT * FROM Customers C WHERE C.cust_id = {cust_id}".format(cust_id=customer_id))
        conn.commit()

    except Exception as e:
        return Customer.bad_customer()

    finally:
        conn.close()
        if result.rows: # TODO: why is this in the 'finally'? Isn't that going to execute regardless of an exception?
            return Customer(result.rows[0][0], result.rows[0][1])
        return Customer.bad_customer()


def delete_customer(customer_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Customers C "
                        "WHERE C.cust_id = {cust_id}").format(cust_id=sql.Literal(customer_id))
        rows_effected, result = conn.execute(query)
        conn.commit()

    except Exception as e:
        return ReturnValue.ERROR

	# TODO: what about BAD_PARAMS?

    finally:
        conn.close()

    if rows_effected == 0:
        return ReturnValue.NOT_EXISTS

    return ReturnValue.OK


def customer_made_reservation(customer_id: int, apartment_id: int, start_date: date, end_date: date,
                              total_price: float) -> ReturnValue:
	conn = None
	try:
		conn = Connector.DBConnector()

		# A bit complicated: there isn't a FROM clause here.
		# Basically, 'WHERE NOT EXISTS' is true when there's no overlapping, and in that case, 'select' will just create a row on the fly
		# 'WHERE NOT EXISTS' is false when there is overlapping, and in that case the entire subquery will return an empty relation
		rows_effected, _ = conn.execute("INSERT INTO Reserves "
						"SELECT {customer_id},{apartment_id},{start_date},{end_date},{total_price}"
						"WHERE NOT EXISTS("
						"SELECT 1 FROM Reserves r WHERE r.apartment_id = {apartment_id} AND (r.start_date, r.end_date) OVERLAPS ({start_date}, {end_date}) )"
							.format(customer_id=customer_id, apartment_id=apartment_id, start_date=start_date, end_date=end_date, total_price=total_price))
		conn.commit()

	except DatabaseException.NOT_NULL_VIOLATION as e:
		return ReturnValue.BAD_PARAMS
	except DatabaseException.CHECK_VIOLATION as e:
		return ReturnValue.BAD_PARAMS
	#except DatabaseException.UNIQUE_VIOLATION as e: TODO: for Shilo, think if this table should have keys.
	#	return ReturnValue.ALREADY_EXISTS
	#except DatabaseException.FOREIGN_KEY_VIOLATION as e:
	#	return ReturnValue.ALREADY_EXISTS
	except DatabaseException.FOREIGN_KEY_VIOLATION as e:
		return ReturnValue.NOT_EXISTS
	except DatabaseException.ConnectionInvalid as e:
		return ReturnValue.ERROR
	except Exception as e:
		return ReturnValue.ERROR
	finally:
		conn.close()

	if rows_effected == 0: # In case of dates overlapping
		return ReturnValue.BAD_PARAMS

	return ReturnValue.OK


def customer_cancelled_reservation(customer_id: int, apartment_id: int, start_date: date) -> ReturnValue:
	conn = None
	try:
		conn = Connector.DBConnector()
		rows_effected, _ = (conn.execute("DELETE FROM Reserves "
						"WHERE Reserves.cust_id = {customer_id} AND Reserves.apartment_id = {apartment_id} AND Reserves.start_date = {start_date}")
								 .format(customer_id=customer_id,apartment_id=apartment_id,start_date=start_date.strftime('%Y-%m-%d')))
		conn.commit()

	except DatabaseException.NOT_NULL_VIOLATION as e:
		return ReturnValue.BAD_PARAMS
	except DatabaseException.CHECK_VIOLATION as e:
		return ReturnValue.BAD_PARAMS
	except DatabaseException.FOREIGN_KEY_VIOLATION as e:
		return ReturnValue.NOT_EXISTS
	except DatabaseException.ConnectionInvalid as e:
		return ReturnValue.ERROR
	except Exception as e:
		return ReturnValue.ERROR

	finally:
		conn.close()

	if rows_effected == 0:
		return ReturnValue.NOT_EXISTS

	return ReturnValue.OK


def customer_reviewed_apartment(customer_id: int, apartment_id: int, review_date: date, rating: int,
                                review_text: str) -> ReturnValue:
	conn = None
	try:
		conn = Connector.DBConnector()

		# Same shtick as with 'customer_made_reservation()'
		# 'WHERE EXISTS' is true when there's a reservation that ended before 'review_date', and in that case, 'select' will just create a row on the fly
		# 'WHERE EXISTS' is false otherwise, and in that case the entire subquery will return an empty relation
		(conn.execute("INSERT INTO Reviews "
						"SELECT {customer_id},{apartment_id},{review_date},{rating},{review_text}"
					  	"WHERE EXISTS("
					  	"SELECT 1 FROM Reserves r WHERE r.cust_id = {customer_id} AND r.apartment_id = {apartment_id} AND r.end_date < {review_date}")
								 .format(customer_id=customer_id,apartment_id=apartment_id,review_date=review_date.strftime('%Y-%m-%d'),rating=rating,review_text=review_text))
		conn.commit()

	except DatabaseException.NOT_NULL_VIOLATION as e:
		return ReturnValue.BAD_PARAMS
	except DatabaseException.CHECK_VIOLATION as e:
		return ReturnValue.BAD_PARAMS
	except DatabaseException.FOREIGN_KEY_VIOLATION as e:
		return ReturnValue.NOT_EXISTS
	except DatabaseException.UNIQUE_VIOLATION as e:
		return ReturnValue.ALREADY_EXISTS
	except DatabaseException.ConnectionInvalid as e:
		return ReturnValue.ERROR
	except Exception as e:
		return ReturnValue.ERROR

	finally:
		conn.close()

	return ReturnValue.OK


def customer_updated_review(customer_id: int, apartment_id: int, update_date: date, new_rating: int,
                            new_text: str) -> ReturnValue:
	conn = None
	try:
		conn = Connector.DBConnector()
		rows_effected, _ = (conn.execute("UPDATE Reviews "
						"SET rating={new_rating}, review_text={new_text}"
						"WHERE cust_id={customer_id} AND apartment_id={apartment_id} AND review_date < {update_date}")
								 .format(new_rating=new_rating,new_text=new_text,customer_id=customer_id,apartment_id=apartment_id,update_date=update_date.strftime('%Y-%m-%d')))
		conn.commit()

	except DatabaseException.NOT_NULL_VIOLATION as e:
		return ReturnValue.BAD_PARAMS
	except DatabaseException.CHECK_VIOLATION as e:
		return ReturnValue.BAD_PARAMS
	except DatabaseException.ConnectionInvalid as e:
		return ReturnValue.ERROR
	except Exception as e:
		return ReturnValue.ERROR

	finally:
		conn.close()

	if rows_effected == 0:
		return ReturnValue.NOT_EXISTS

	return ReturnValue.OK


def owner_owns_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()

        query_str = sql.SQL("INSERT INTO Owns(apartment_id,owner_id) "
                            "SELECT {apartment_id},{owner_id} "
                            "WHERE EXISTS (SELECT 1 FROM owners WHERE owner_id = {owner_id});").format(
                        apartment_id=sql.Literal(apartment_id),
                        owner_id=sql.Literal(owner_id))

        rows_effected, result = conn.execute(query_str)
        conn.commit()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except Exception as e:
        return ReturnValue.ERROR
    finally:
        conn.close()

    if rows_effected == 0:
        return ReturnValue.NOT_EXISTS

    return ReturnValue.OK


def owner_drops_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Owns "
                        "WHERE Owns.owner_id = {owner_id} AND Owns.apartment_id = {apartment_id} ").format(owner_id=sql.Literal(owner_id),apartment_id=sql.Literal(apartment_id))
        rows_effected, result = conn.execute(query)
        conn.commit()

    except DatabaseException.NOT_NULL_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return ReturnValue.NOT_EXISTS
    except Exception as e:
        return ReturnValue.ERROR

    finally:
        conn.close()

    if rows_effected == 0:
        return ReturnValue.NOT_EXISTS

    return ReturnValue.OK

def get_apartment_owner(apartment_id: int) -> Owner:
    conn = None
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute(
            "SELECT Owns.owner_id,Owners.owner_name FROM Owns,Owners "
            "WHERE Owns.apartment_id = {apartment_id} AND Owners.owner_id = Owns.owner_id;".format(apartment_id=apartment_id))
        conn.commit()

    except Exception as e:
        return Owner.bad_owner()

    finally:
        conn.close()
        if result.rows: # TODO: why is this in the 'finally'? Isn't that going to execute regardless of an exception?
            return Owner(result.rows[0][0], result.rows[0][1]) # TODO: wait, why are you addressing result using 'rows'?
        return Owner.bad_owner()


def get_owner_apartments(owner_id: int) -> List[Apartment]:
    conn = None
    apartments = []
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute(
            "SELECT Apartments.* FROM Apartments, Owns "
            "WHERE Owns.owner_id = {owner_id} AND Apartments.apartment_id = Owns.apartment_id;".format(owner_id=owner_id))
        conn.commit()

    except Exception as e:
        return apartments

    finally:
        conn.close()

    # build the list of apartments.
    for index in range(rows_effected):
        apartments.append(Apartment(result.rows[index][0], result.rows[index][1], result.rows[index][2], result.rows[index][3],
                             result.rows[index][4]))
    return apartments



# ---------------------------------- BASIC API: ----------------------------------

def get_apartment_rating(apartment_id: int) -> float:
    # TODO: implement
    pass


def get_owner_rating(owner_id: int) -> float:
    # TODO: implement
    pass


def get_top_customer() -> Customer:
	conn = None
	try:
		conn = Connector.DBConnector()
		# We group 'Reserves' by 'cust_id', and then sort it:
		# First by group's count in descending order (so bigger is first), and if there's a tie - then by 'cust_id' in ascending order (so smaller is first)
		# Then we limit only to the first tuple (we only want the top customer)
		_, result = conn.execute("SELECT cust_id"
									"FROM Reserves"
									"GROUP BY cust_id"
									"ORDER BY COUNT(*) DESC, cust_id"
									"LIMIT 1")
		conn.commit()

		return Customer(result[0]['cust_id'], result[0]['cust_name'])

	except Exception as e:
		return ReturnValue.ERROR

	finally:
		conn.close()


def reservations_per_owner() -> List[Tuple[str, int]]:
	conn = None
	try:
		conn = Connector.DBConnector()
		# We want num_of_reservations for *all* owners, not just ones with actual reservations.
		# Because of that, we first use right outer join, and only then we group by 'owner_id'
		_, result = conn.execute("SELECT owner_id, COUNT(*) AS num_of_reservations"
									"FROM Reserves r RIGHT OUTER JOIN Owns o ON r.apartment.id = o.apartment"
									"GROUP BY o.owner_id")
		conn.commit()

		return result

	except Exception as e:
		return ReturnValue.ERROR

	finally:
		conn.close()


# ---------------------------------- ADVANCED API: ----------------------------------

def get_all_location_owners() -> List[Owner]:
    # TODO: implement
    pass


def best_value_for_money() -> Apartment:
    # TODO: implement
    pass


def profit_per_month(year: int) -> List[Tuple[int, float]]:
    # TODO: implement
    pass


def get_apartment_recommendation(customer_id: int) -> List[Tuple[Apartment, float]]:
    # TODO: implement
    pass

dropTables()
createTables()
add_customer(Customer(123, "David"))
add_customer(Customer(222, "Yossi"))