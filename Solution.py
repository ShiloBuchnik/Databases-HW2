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
                     "FOREIGN KEY (apartment_id) REFERENCES Apartments(apartment_id));"


                     # "FOREIGN KEY (owner_id) REFERENCES Owners(owner_id)"
                     # "ON DELETE CASCADE)"
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
        conn.execute("TRUNCATE Owners, Apartments, Customers, Owns")
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
        rows_effected, result = conn.execute(
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
        return ReturnValue.NOT_EXISTS

    return ReturnValue.OK


def customer_made_reservation(customer_id: int, apartment_id: int, start_date: date, end_date: date,
                              total_price: float) -> ReturnValue:
    # TODO: implement
    # Shilo
    pass


def customer_cancelled_reservation(customer_id: int, apartment_id: int, start_date: date) -> ReturnValue:
    # TODO: implement
    # Shilo

    pass


def customer_reviewed_apartment(customer_id: int, apartment_id: int, review_date: date, rating: int,
                                review_text: str) -> ReturnValue:
    # TODO: implement
    # Shilo

    pass


def customer_updated_review(customer_id: int, apartmetn_id: int, update_date: date, new_rating: int,
                            new_text: str) -> ReturnValue:
    # TODO: implement
    # Shilo

    pass


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
    # TODO: implement
    pass


def reservations_per_owner() -> List[Tuple[str, int]]:
    # TODO: implement
    pass


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
