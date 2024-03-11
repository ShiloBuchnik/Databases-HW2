from psycopg2 import sql
from datetime import date, datetime
from typing import List, Tuple

import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException

from Business.Owner import Owner
from Business.Customer import Customer
from Business.Apartment import Apartment


# ---------------------------------- CRUD API: ----------------------------------

def create_tables():
	conn = None
	try:
		conn = Connector.DBConnector()
		conn.execute(

					 "CREATE TABLE Owners("
					 "owner_id INTEGER PRIMARY KEY NOT NULL CHECK(owner_id > 0),"
					 "owner_name TEXT NOT NULL);"

					 "CREATE TABLE Apartments("
					 "apartment_id INTEGER PRIMARY KEY NOT NULL CHECK(apartment_id > 0),"
					 "address TEXT NOT NULL,"
					 "city TEXT NOT NULL,"
					 "country TEXT NOT NULL,"
					 "size INTEGER NOT NULL CHECK(size > 0), "
					 "UNIQUE (Address, City, Country)"
					 ");"

					 "CREATE TABLE Customers( "
					 "cust_id INTEGER PRIMARY KEY NOT NULL CHECK(cust_id > 0),"
					 "cust_name TEXT NOT NULL);"

					 "CREATE TABLE Owns( "
					 "apartment_id INTEGER PRIMARY KEY NOT NULL CHECK(apartment_id > 0),"
					 "owner_id INTEGER NOT NULL CHECK(owner_id > 0),"
					 "FOREIGN KEY (owner_id) REFERENCES Owners(owner_id) ON DELETE CASCADE,"
					 "FOREIGN KEY (apartment_id) REFERENCES Apartments(apartment_id) ON DELETE CASCADE);"


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

					# Creating this view "globally", since we need it for 'get_apartment_rating' and 'get_owner_rating' as well
					# It returns a table with all apartments and their average rating. If an apartment doesn't have ratings - its average rating is 0.
					"CREATE VIEW AllApartmentsRating AS "
					"SELECT a.apartment_id as id, COALESCE(AVG(r.rating), 0) AS rating "
					"FROM Apartments a LEFT JOIN Reviews r ON a.apartment_id = r.apartment_id "
					"GROUP BY a.apartment_id; ")

		conn.commit()

	except Exception as e:
		print(e)

	finally:
		conn.close()


def clear_tables():
	conn = None
	try:
		conn = Connector.DBConnector()
		conn.execute("TRUNCATE Owners, Apartments, Customers, Owns, Reviews, Reserves")
		conn.commit()

	except Exception as e:
		print(e)

	finally:
		conn.close()


def drop_tables():
	conn = None
	try:
		conn = Connector.DBConnector()
		conn.execute(
					 "DROP VIEW IF EXISTS AllApartmentsRating CASCADE; "

					 "DROP TABLE IF EXISTS Owners CASCADE;"
					 "DROP TABLE IF EXISTS Apartments CASCADE;"
					 "DROP TABLE IF EXISTS Customers CASCADE;"
					 "DROP TABLE IF EXISTS Owns CASCADE;"
					 "DROP TABLE IF EXISTS Reviews CASCADE;"
					 "DROP TABLE IF EXISTS Reserves CASCADE;")
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


def get_owner(owner_id: int) -> Owner:
	conn = None
	try:
		conn = Connector.DBConnector()
		_, result = conn.execute(
			"SELECT * FROM Owners WHERE Owners.owner_id = {owner_id}".format(owner_id=owner_id))
		conn.commit()

	except Exception as e:
		return Owner.bad_owner()

	finally:
		conn.close()

	if result.rows:
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


	finally:
		conn.close()

	if rows_effected == 0:
		if owner_id > 0:
			return ReturnValue.NOT_EXISTS
		return ReturnValue.BAD_PARAMS

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
		_, _ = conn.execute(query)
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
	if result.rows:
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

	finally:
		conn.close()

	if rows_effected == 0:
		if apartment_id > 0:
			return ReturnValue.NOT_EXISTS
		return ReturnValue.BAD_PARAMS

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
	if result.rows:
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

	finally:
		conn.close()

	if rows_effected == 0:
		if customer_id > 0:
			return ReturnValue.NOT_EXISTS
		return ReturnValue.BAD_PARAMS

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
										"SELECT {customer_id},{apartment_id},'{start_date}','{end_date}',{total_price} "
										"WHERE NOT EXISTS("
										"SELECT 1 FROM Reserves r WHERE r.apartment_id = {apartment_id} AND (r.start_date, r.end_date) OVERLAPS ('{start_date}', '{end_date}') )"
										.format(customer_id=customer_id, apartment_id=apartment_id,
												start_date=start_date.strftime('%Y-%m-%d'),
												end_date=end_date.strftime('%Y-%m-%d'), total_price=total_price))
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

	if rows_effected == 0:  # In case of dates overlapping
		return ReturnValue.BAD_PARAMS

	return ReturnValue.OK


def customer_cancelled_reservation(customer_id: int, apartment_id: int, start_date: date) -> ReturnValue:
	# If SQL will search the table for a tuple with these bad parameters, it will find nothing and return 'NOT EXISTS'
	# And while it is true that it doesn't exist, we want to inform that those are bad parameters, so we perform this check
	if customer_id <= 0 or apartment_id <= 0:
		return ReturnValue.BAD_PARAMS

	conn = None
	try:
		conn = Connector.DBConnector()
		rows_effected, _ = conn.execute("DELETE FROM Reserves "
										"WHERE Reserves.cust_id = {customer_id} AND Reserves.apartment_id = {apartment_id} AND Reserves.start_date = '{start_date}'"
										.format(customer_id=customer_id, apartment_id=apartment_id,
												start_date=start_date.strftime('%Y-%m-%d')))
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
	# Insert is conditional, so if condition isn't met, we might ignore BAD_PARAMS (because we didn't insert them, so we wouldn't get an exception)
	# For that, we check beforehand
	if customer_id <= 0 or apartment_id <= 0 or rating not in range(1, 11):
		return ReturnValue.BAD_PARAMS

	conn = None
	try:
		conn = Connector.DBConnector()

		# Same shtick as with 'customer_made_reservation()'
		# 'WHERE EXISTS' is true when there's a reservation that ended before 'review_date', and in that case, 'select' will just create a row on the fly
		# 'WHERE EXISTS' is false otherwise, and in that case the entire subquery will return an empty relation
		rows_effected, _ = conn.execute("INSERT INTO Reviews "
					 "SELECT {customer_id},{apartment_id},'{review_date}',{rating},'{review_text}' "
					 "WHERE EXISTS("
					 "SELECT 1 FROM Reserves r WHERE r.cust_id = {customer_id} AND r.apartment_id = {apartment_id} AND r.end_date <= '{review_date}')"
					 .format(customer_id=customer_id, apartment_id=apartment_id,
							 review_date=review_date.strftime('%Y-%m-%d'), rating=rating, review_text=review_text))
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

	if rows_effected:
		return ReturnValue.OK
	else:
		return ReturnValue.NOT_EXISTS


def customer_updated_review(customer_id: int, apartment_id: int, update_date: date, new_rating: int,
							new_text: str) -> ReturnValue:
	# Same deal as with 'customer_reviewed_apartment()'
	if customer_id <= 0 or apartment_id <= 0 or new_rating <= 0 or new_rating > 10:
		return ReturnValue.BAD_PARAMS

	conn = None
	try:
		conn = Connector.DBConnector()
		rows_effected, _ = conn.execute("UPDATE Reviews "
										"SET review_date = '{update_date}', rating={new_rating}, review_text='{new_text}' "
										"WHERE cust_id={customer_id} AND apartment_id={apartment_id} AND review_date <= '{update_date}'"
										.format(new_rating=new_rating, new_text=new_text, customer_id=customer_id,
												apartment_id=apartment_id,
												update_date=update_date.strftime('%Y-%m-%d')))
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
		if owner_id > 0:
			return ReturnValue.NOT_EXISTS
		return ReturnValue.BAD_PARAMS

	return ReturnValue.OK

def owner_drops_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
	conn = None
	try:
		conn = Connector.DBConnector()
		query = sql.SQL("DELETE FROM Owns "
						"WHERE Owns.owner_id = {owner_id} AND Owns.apartment_id = {apartment_id} ").format(
			owner_id=sql.Literal(owner_id), apartment_id=sql.Literal(apartment_id))
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
		if owner_id > 0 and apartment_id > 0:
			return ReturnValue.NOT_EXISTS
		return ReturnValue.BAD_PARAMS

	return ReturnValue.OK


def get_apartment_owner(apartment_id: int) -> Owner:
	conn = None
	try:
		conn = Connector.DBConnector()
		rows_effected, result = conn.execute(
			"SELECT Owns.owner_id,Owners.owner_name FROM Owns,Owners "
			"WHERE Owns.apartment_id = {apartment_id} AND Owners.owner_id = Owns.owner_id;".format(
				apartment_id=apartment_id))
		conn.commit()

	except Exception as e:
		return Owner.bad_owner()

	finally:
		conn.close()
	if result.rows:
		return Owner(result.rows[0][0], result.rows[0][1])
	return Owner.bad_owner()


def get_owner_apartments(owner_id: int) -> List[Apartment]:
	conn = None
	apartments = []
	try:
		conn = Connector.DBConnector()
		rows_effected, result = conn.execute(
			"SELECT Apartments.* FROM Apartments, Owns "
			"WHERE Owns.owner_id = {owner_id} AND Apartments.apartment_id = Owns.apartment_id;".format(
				owner_id=owner_id))
		conn.commit()

	except Exception as e:
		return apartments

	finally:
		conn.close()

	# build the list of apartments.
	for index in range(rows_effected):
		apartments.append(
			Apartment(result.rows[index][0], result.rows[index][1], result.rows[index][2], result.rows[index][3],
					  result.rows[index][4]))
	return apartments


# ---------------------------------- BASIC API: ----------------------------------

def get_apartment_rating(apartment_id: int) -> float:
	conn = None
	try:
		conn = Connector.DBConnector()

		# In each function call, we drop the view from last function call (if exists), and create a new one to use
		rows_affected, result = conn.execute("SELECT rating "
								 "FROM AllApartmentsRating "
								 "WHERE id = {apartment_id} ".format(apartment_id=apartment_id))

		conn.commit()
		return result[0]['rating']

	except Exception as e:
		return ReturnValue.ERROR

	finally:
		conn.close()

def get_owner_rating(owner_id: int) -> float:
	conn = None
	try:
		conn = Connector.DBConnector()

		_, result = conn.execute(
								 "SELECT COALESCE(AVG(rating), 0) AS average_rating "
								 "FROM AllApartmentsRating AAR JOIN Owns o ON AAR.id = o.apartment_id "
								 "WHERE o.owner_id = {owner_id}; ".format(owner_id=owner_id))

		conn.commit()
		return result[0]['average_rating']

	except Exception as e:
		return ReturnValue.ERROR

	finally:
		conn.close()


def get_top_customer() -> Customer:
	conn = None
	try:
		conn = Connector.DBConnector()
		# We group 'Reserves' by 'cust_id', and then sort it:
		# First by group's count in descending order (so bigger is first), and if there's a tie - then by 'cust_id' in ascending order (so smaller is first)
		# Then we limit only to the first tuple (we only want the top customer)

		# Minor tidbit: 'Reserves' only gives us 'cust_id', but we need 'cust_name' as well;
		# so we query in 'Customers' using the returned 'cust_id' from the subquery
		_, result = conn.execute("SELECT cust_id, cust_name "
								 "FROM Customers as c "
								 "WHERE c.cust_id = "
								 "(SELECT cust_id "
								 "FROM Reserves "
								 "GROUP BY cust_id "
								 "ORDER BY COUNT(*) DESC, cust_id "
								 "LIMIT 1)")
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
		_, result = conn.execute("SELECT o.owner_name, COALESCE(COUNT(r.apartment_id), 0) AS numberOfReservations "
								 "FROM Owners o "
								 "LEFT JOIN Owns ow ON o.owner_id = ow.owner_id "
								 "LEFT JOIN Reserves r ON ow.apartment_id = r.apartment_id "
								 "GROUP BY o.owner_name;")
		conn.commit()


		return result.rows

	except Exception as e:
		return ReturnValue.ERROR

	finally:
		conn.close()


# ---------------------------------- ADVANCED API: ----------------------------------

def get_all_location_owners() -> List[Owner]:
	conn = None
	try:
		conn = Connector.DBConnector()

		rows_effected, result = conn.execute(
			"DROP VIEW IF EXISTS AllCityCountryCombinations CASCADE; "
			"DROP VIEW IF EXISTS CityCountryPerOwner CASCADE;"

			"CREATE VIEW AllCityCountryCombinations AS "
			"SELECT DISTINCT city, country "
			"FROM Apartments; "

			"CREATE VIEW CityCountryPerOwner AS "
			"SELECT Owns.owner_id, Apartments.city, Apartments.country "
			"FROM Apartments "
			"JOIN Owns ON Apartments.apartment_id = Owns.apartment_id;"

			"SELECT ccpo.owner_id, o.owner_name "
			"FROM CityCountryPerOwner ccpo "
			"JOIN Owners o ON ccpo.owner_id = o.owner_id "
			"WHERE (ccpo.city, ccpo.country) IN (SELECT city, country FROM AllCityCountryCombinations) "
			"GROUP BY ccpo.owner_id, o.owner_name "
			"HAVING COUNT(DISTINCT ccpo.city || ', ' || ccpo.country) = (SELECT COUNT(*) FROM AllCityCountryCombinations); ")

		conn.commit()

		# JUST convert the result from ResultSet to list
		list_to_ret = []
		for index in range(rows_effected):
			list_to_ret.append(Owner(result.rows[index][0],result.rows[index][1]))
		return list_to_ret

	except Exception as e:
		return ReturnValue.ERROR

	finally:
		conn.close()


def best_value_for_money() -> Apartment:
	conn = None
	try:
		conn = Connector.DBConnector()
		rows_effected, result = conn.execute(
			"DROP VIEW IF EXISTS AverageRatingPerApartment CASCADE; "
			"DROP VIEW IF EXISTS AverageCostPerApartment CASCADE;"

			"CREATE VIEW AverageRatingPerApartment AS "
			"SELECT apartment_id, AVG(rating) avg_rating "
			"FROM Reviews "
			"GROUP BY apartment_id;"

			"CREATE VIEW AverageCostPerApartment AS "
			"SELECT apartment_id, AVG(total_price / (end_date - start_date)) avg_cost "
			"FROM Reserves "
			"GROUP BY apartment_id;"

			"SELECT c.apartment_id, a.address, a.city, a.country, a.size, COALESCE(r.avg_rating, 0) / c.avg_cost AS review_cost_ratio "
			"FROM AverageCostPerApartment c "
			"LEFT JOIN AverageRatingPerApartment r ON c.apartment_id = r.apartment_id "
			"JOIN Apartments a ON c.apartment_id = a.apartment_id "
			"ORDER BY review_cost_ratio DESC "
			"LIMIT 1; "

		)

		conn.commit()
		return Apartment(result.rows[0][0],result.rows[0][1],result.rows[0][2],result.rows[0][3],result.rows[0][4]) if rows_effected else Apartment.bad_apartment()

	except Exception as e:
		return ReturnValue.ERROR

	finally:
		conn.close()


# Note to self: in a transaction (bunch of statements) PostgreSQL returns the return value of the last statement
# So if the last statement is 'COMMIT', it will return its return value, which is an *empty table*.
# Also, we don't even need to write 'COMMIT' anyway, it is done automatically by the 'DBConnector' class
def profit_per_month(year: int) -> List[Tuple[int, float]]:
	conn = None
	try:
		conn = Connector.DBConnector()

		# In each function call, we drop the view from last function call (if exists), and create a new one to use
		_, result = conn.execute("DROP VIEW IF EXISTS MonthsView CASCADE;"
								 "DROP VIEW IF EXISTS ApartmentsInYear CASCADE; "

								 "CREATE VIEW MonthsView AS "
                                "SELECT 1 AS MonthNumber "
                                "UNION ALL "
                                "SELECT 2 "
                                "UNION ALL "
                                "SELECT 3 "
                                "UNION ALL "
                                "SELECT 4 "
                                "UNION ALL "
                                "SELECT 5 "
                                "UNION ALL "
                                "SELECT 6 "
                                "UNION ALL "
                                "SELECT 7 "
                                "UNION ALL "
                                "SELECT 8 "
                                "UNION ALL "
                                "SELECT 9 "
                                "UNION ALL "
                                "SELECT 10 "
                                "UNION ALL "
                                "SELECT 11 "
                                "UNION ALL "
                                "SELECT 12; "

                                 "CREATE VIEW ApartmentsInYear AS "
                                 "SELECT total_price, EXTRACT(MONTH FROM (end_date)) AS month "
                                 "FROM Reserves "
                                 "WHERE {year} = EXTRACT(YEAR FROM (end_date)); "

                                 "SELECT MonthNumber, CAST(0.15*(SUM(COALESCE(total_price,0))) AS FLOAT) "
                                 "FROM ApartmentsInYear AIY "
                                 "RIGHT OUTER JOIN MonthsView NV ON AIY.month = NV.MonthNumber "
                                 "GROUP BY MonthNumber " 
                                 "ORDER BY MonthNumber; ".format(year=year))

		conn.commit()
		return result.rows

	except Exception as e:
		return ReturnValue.ERROR

	finally:
		conn.close()


# GOD FUCKING DAMMIT A CUSTOMER CAN ONLY REVIEW AN APARTMENT *ONCE* THIS CHANGES EVERYTHING AAAAAAAAAAAAAAAAAAAAAAAA (i luv snakes)
def get_apartment_recommendation(customer_id: int) -> List[Tuple[Apartment, float]]:
	conn = None
	try:
		conn = Connector.DBConnector()

		_, result = conn.execute("DROP VIEW IF EXISTS ReducedReviews CASCADE; "
								 "DROP VIEW IF EXISTS JoinedWithRatios CASCADE; "
								 "DROP VIEW IF EXISTS averageRatioPerCustomer CASCADE; "
								 "DROP VIEW IF EXISTS approximationPerApartment CASCADE; "

								 # Reducing 'Reviews' to include only tuples with apartments that 'customer_id' has reviewed
								 "CREATE VIEW ReducedReviews AS "
								 "SELECT * "
								 "FROM Reviews "
								 "WHERE apartment_id IN ( "
								 "SELECT apartment_id FROM Reviews WHERE cust_id = {customer_id}); "

								 # Joining 'ReducedReviews' with itself, and getting all the ratios for each customer
								 "CREATE VIEW JoinedWithRatios AS "
								 "SELECT r1.cust_id AS r1_cust_id, r1.apartment_id, r2.cust_id AS r2_cust_id, (r1.rating * 1.0 / r2.rating * 1.0) AS ratio " # Multiplying by 1.0 for double promotion
								 "FROM ReducedReviews r1 JOIN ReducedReviews r2 ON r1.apartment_id = r2.apartment_id "
								 "WHERE r1.cust_id = {customer_id}; "

								 # Taking average of all said ratios for each customer
								 # Now we have a table of 2 columns: cust_id and its average ratio
								 "CREATE VIEW averageRatioPerCustomer AS "
								 "SELECT r2_cust_id AS cust_id, AVG(ratio) AS average_ratio "
								 "FROM JoinedWithRatios "
								 "GROUP BY r2_cust_id; "


								 # Here's where the magic happens - we join 'Reviews' with 'averageRatioPerCustomer' based on 'cust_id',
								 # and take only tuples that DON'T include apartments that 'customer_id' reviewed
								 # Then, we group by 'apartment_id' (becuase multiple approximations can occur),
								 # and calculate the approximation for each apartment
								 "CREATE VIEW approximationPerApartment AS "
								 "SELECT apartment_id, AVG(LEAST(GREATEST(average_ratio * rating,1),10)) as approximation " # Keeping each approx in legal rating range
								 "FROM Reviews r JOIN averageRatioPerCustomer ARPC ON r.cust_id = ARPC.cust_id "
								 "WHERE r.apartment_id NOT IN ("
								 "SELECT apartment_id FROM Reviews WHERE cust_id = {customer_id}) "
								 "GROUP BY r.apartment_id; "

								 # This is just a formality - we need an apartment object, not just the apartment id, so we join with 'Apartments'
								 "SELECT a.apartment_id AS id, a.address AS address, a.city AS city, a.country AS country, a.size AS size, APA.approximation AS approximation "
								 "FROM Apartments a JOIN approximationPerApartment APA ON a.apartment_id = APA.apartment_id ".format(customer_id=customer_id))

		conn.commit()
		return [ (Apartment(row['id'], row['address'], row['city'], row['country'], row['size']), float(row['approximation'])) for row in result ]

	except Exception as e:
		return ReturnValue.ERROR

	finally:
		conn.close()